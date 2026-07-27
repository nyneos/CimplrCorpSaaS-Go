package fundplanning

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fundPlanningRow is the canonical business-field shape for FUND_PLANNING
// policy checks.
//
// Real table (confirmed via psql, schema "public" — the tracker's guess of
// "fund_plan_allocations" as the master table was wrong on both the schema
// prefix and which table is the master): public.fund_plan_groups. It has no
// FK to a single "plan" header row — plan_id/entity_name/horizon are
// denormalized onto every group row that shares a plan_id. Child tables:
// public.fund_plan_lines (per-line detail), public.fund_plan_allocations
// (per-account allocation), public.fund_plan_files (attachments). Lifecycle/
// audit trail lives in public.auditaction_fund_plan_groups, keyed by
// group_id.
//
// Crucially, every real mutation entrypoint (CreateFundPlan,
// BulkApproveFundPlans, BulkRejectFundPlans, BulkRequestDeleteFundPlans)
// operates at plan_id granularity, not group_id: Create inserts N groups for
// one plan in a single call: Bulk* act on every group in a plan at once.
// There is no single-group create/approve/reject/delete endpoint. So the
// canonical row here is a plan-level aggregate over its fund_plan_groups
// rows (matching the aggregation GetFundPlanSummary already does for
// display), not a literal one-to-one struct over a single table row.
//
// Field set mirrors the 12 field_codes already seeded under
// domain_catalog.field for FUND_PLANNING (verified live via psql) — do not
// invent new field_codes. Before this file: CreateFundPlan's Enforce call
// passed only {entity_name, horizon, group_count} (no plan_id at all, since
// planID is generated before the Enforce call but was never added to the
// Fields map); BulkApproveFundPlans/BulkRejectFundPlans/
// BulkRequestDeleteFundPlans each passed only {plan_id}. A policy checking
// e.g. currency or total_amount would never fire on any of the four real
// mutation handlers — a BANK_BALANCE-style consistency gap, not just a
// coverage gap.
type fundPlanningRow struct {
	PlanID       string
	GroupID      string // distinct group_ids in the plan, comma-joined
	EntityName   string
	Horizon      int
	Direction    string // distinct directions in the plan, comma-joined
	Currency     string // distinct currencies in the plan, comma-joined
	PrimaryKey   string // distinct primary_key values in the plan, comma-joined
	PrimaryValue string // distinct primary_value values in the plan, comma-joined
	TotalAmount  float64
	GroupCount   int
	// CounterpartyName mirrors what validateFundPlanScope already extracts
	// per-group (primary_value when primary_key == "Counterparty"),
	// aggregated across every such group in the plan.
	CounterpartyName string
	// AccountIDs mirrors the account_ids field already built ad hoc in
	// CreateFundPlan's scope-validation loop (allocated_accounts +
	// per-line allocated_bank_account) — for a loaded plan, sourced from
	// fund_plan_allocations instead since that's the persisted record.
	AccountIDs []string
}

// buildFundPlanningPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for FUND_PLANNING (see
// cmd/seedDomainCatalog/seeds.go's FUND_PLANNING block +
// fundPlanningCanonical.go's cdm_path backfill).
func buildFundPlanningPolicyFields(row fundPlanningRow) map[string]interface{} {
	return map[string]interface{}{
		"plan_id":           row.PlanID,
		"group_id":          row.GroupID,
		"entity_name":       row.EntityName,
		"horizon":           row.Horizon,
		"direction":         row.Direction,
		"currency":          row.Currency,
		"primary_key":       row.PrimaryKey,
		"primary_value":     row.PrimaryValue,
		"total_amount":      row.TotalAmount,
		"group_count":       row.GroupCount,
		"counterparty_name": row.CounterpartyName,
		"account_ids":       row.AccountIDs,
	}
}

// loadFundPlanningRow fetches the plan-level aggregate by plan_id — used by
// BulkApproveFundPlans/BulkRejectFundPlans/BulkRequestDeleteFundPlans, which
// only ever receive a plan_id in the request, never the group data itself.
func loadFundPlanningRow(ctx context.Context, pool *pgxpool.Pool, planID string) (fundPlanningRow, error) {
	var row fundPlanningRow
	row.PlanID = planID
	err := pool.QueryRow(ctx, `
		SELECT
			COALESCE(MAX(fpg.entity_name), ''),
			COALESCE(MAX(fpg.horizon), 0),
			COUNT(*),
			COALESCE(SUM(fpg.total_amount), 0),
			COALESCE(STRING_AGG(DISTINCT fpg.group_id, ', '), ''),
			COALESCE(STRING_AGG(DISTINCT fpg.direction, ', '), ''),
			COALESCE(STRING_AGG(DISTINCT fpg.currency, ', '), ''),
			COALESCE(STRING_AGG(DISTINCT fpg.primary_key, ', '), ''),
			COALESCE(STRING_AGG(DISTINCT fpg.primary_value, ', '), ''),
			COALESCE(STRING_AGG(DISTINCT CASE WHEN fpg.primary_key = 'Counterparty' THEN fpg.primary_value END, ', '), '')
		FROM public.fund_plan_groups fpg
		WHERE fpg.plan_id = $1
		GROUP BY fpg.plan_id`, planID,
	).Scan(&row.EntityName, &row.Horizon, &row.GroupCount, &row.TotalAmount,
		&row.GroupID, &row.Direction, &row.Currency, &row.PrimaryKey, &row.PrimaryValue,
		&row.CounterpartyName)
	if err != nil {
		return fundPlanningRow{}, fmt.Errorf("load fund plan for policy: %w", err)
	}

	accErr := pool.QueryRow(ctx, `
		SELECT COALESCE(ARRAY_AGG(DISTINCT fpa.account_id), ARRAY[]::text[])
		FROM public.fund_plan_allocations fpa
		JOIN public.fund_plan_groups fpg ON fpg.group_id = fpa.group_id
		WHERE fpg.plan_id = $1`, planID,
	).Scan(&row.AccountIDs)
	if accErr != nil {
		return fundPlanningRow{}, fmt.Errorf("load fund plan account ids for policy: %w", accErr)
	}

	return row, nil
}

// fundPlanningRowFromCreateRequest builds the canonical row directly from an
// already-parsed CreateFundPlanRequest, for CreateFundPlan — which has the
// full group set in hand pre-insert (real group_ids don't exist yet at
// Enforce time; they're generated per-group inside processGroupCreation
// after the policy check passes), so it builds the row from the request
// instead of loading it back from the DB.
func fundPlanningRowFromCreateRequest(planID string, req CreateFundPlanRequest) fundPlanningRow {
	row := fundPlanningRow{
		PlanID:     planID,
		EntityName: req.EntityName,
		Horizon:    req.Horizon,
		GroupCount: len(req.Groups),
	}

	directions := make([]string, 0, len(req.Groups))
	currencies := make([]string, 0, len(req.Groups))
	primaryKeys := make([]string, 0, len(req.Groups))
	primaryValues := make([]string, 0, len(req.Groups))
	counterpartyNames := make([]string, 0, len(req.Groups))
	accountIDSeen := make(map[string]struct{})
	accountIDs := make([]string, 0)

	for _, group := range req.Groups {
		direction, currency, primaryKey, primaryValue, err := resolveGroupMetadata(group)
		if err != nil {
			// Metadata couldn't be resolved for this group (e.g. malformed
			// group_id) — CreateFundPlan itself re-derives and rejects this
			// case per-group before insert; here we just skip it from the
			// aggregate rather than fail the whole policy field build.
			continue
		}
		row.TotalAmount += group.TotalAmount
		directions = append(directions, direction)
		currencies = append(currencies, currency)
		primaryKeys = append(primaryKeys, primaryKey)
		primaryValues = append(primaryValues, primaryValue)
		if strings.EqualFold(primaryKey, "counterparty") {
			counterpartyNames = append(counterpartyNames, primaryValue)
		}
		for _, allocation := range group.AllocatedAccounts {
			if allocation.AccountID == "" {
				continue
			}
			if _, ok := accountIDSeen[allocation.AccountID]; !ok {
				accountIDSeen[allocation.AccountID] = struct{}{}
				accountIDs = append(accountIDs, allocation.AccountID)
			}
		}
		for _, line := range group.Lines {
			if line.AllocatedBankAccount == "" {
				continue
			}
			if _, ok := accountIDSeen[line.AllocatedBankAccount]; !ok {
				accountIDSeen[line.AllocatedBankAccount] = struct{}{}
				accountIDs = append(accountIDs, line.AllocatedBankAccount)
			}
		}
	}

	row.Direction = joinDistinct(directions)
	row.Currency = joinDistinct(currencies)
	row.PrimaryKey = joinDistinct(primaryKeys)
	row.PrimaryValue = joinDistinct(primaryValues)
	row.CounterpartyName = joinDistinct(counterpartyNames)
	row.AccountIDs = accountIDs
	return row
}

// joinDistinct dedups (preserving first-seen order) and comma-joins, mirroring
// the STRING_AGG(DISTINCT ..., ', ') shape loadFundPlanningRow reads back.
func joinDistinct(vals []string) string {
	seen := make(map[string]struct{}, len(vals))
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return strings.Join(out, ", ")
}
