package sweepconfig

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// sweepExecutionRow is the canonical business-field shape for SWEEP_EXECUTION
// — one field per real scalar column on cimplrcorpsaas.sweep_execution_log,
// plus entity_name, which is not a real column on the log table itself but is
// already seeded in domain_catalog as a denormalized enrichment pulled from
// the parent cimplrcorpsaas.sweepconfiguration row (see
// GetAllSweepExecutionLogsV2's join in sweepExecutorV2.go for precedent).
//
// SWEEP_EXECUTION has exactly one live handler with a runtime.Enforce call —
// ManualTriggerSweepV2 — and it fires *before* the sweep actually executes,
// so execution_id/amount_swept/status/error_message/balance_before/
// balance_after are not yet known at check time and are left zero-valued.
// See the package-level report for two other mutation handlers in
// sweepExecutorV2.go that were found with zero enforcement at all
// (BulkManualTriggerSweepV2WithAutoApproval, live; ManualTriggerSweepV2Direct,
// dead/commented-out) — neither was touched here, per the rollout rule that
// adding enforcement to a previously-unchecked endpoint is a functional/risk
// change, not a field-consistency fix.
type sweepExecutionRow struct {
	ExecutionID   string
	InitiationID  string
	SweepID       string
	EntityName    string
	ExecutionDate string
	AmountSwept   *float64
	FromAccount   string
	ToAccount     string
	Status        string
	ErrorMessage  string
	BalanceBefore *float64
	BalanceAfter  *float64
}

// buildSweepExecutionPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for SWEEP_EXECUTION (see
// cmd/seedDomainCatalog/sweepExecutionCanonical.go).
func buildSweepExecutionPolicyFields(row sweepExecutionRow) map[string]interface{} {
	return map[string]interface{}{
		"execution_id":   row.ExecutionID,
		"initiation_id":  row.InitiationID,
		"sweep_id":       row.SweepID,
		"entity_name":    row.EntityName,
		"execution_date": row.ExecutionDate,
		"amount_swept":   row.AmountSwept,
		"from_account":   row.FromAccount,
		"to_account":     row.ToAccount,
		"status":         row.Status,
		"error_message":  row.ErrorMessage,
		"balance_before": row.BalanceBefore,
		"balance_after":  row.BalanceAfter,
	}
}

// loadSweepExecutionLogRow fetches a full execution log row by execution_id,
// enriched with the parent sweep's entity_name. Not currently wired to any
// call site (no real handler receives only an execution_id and needs a
// reload — ManualTriggerSweepV2, the one enforced handler, already has its
// row's fields in scope before the log row even exists), but provided for
// parity with the other two sub-modules' builder shape and for any future
// ID-only call site (e.g. a would-be retry/resolve action).
func loadSweepExecutionLogRow(ctx context.Context, pool *pgxpool.Pool, executionID string) (sweepExecutionRow, error) {
	var row sweepExecutionRow
	row.ExecutionID = executionID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(l.initiation_id::text,''), l.sweep_id, COALESCE(c.entity_name,''),
		       l.execution_date::text, l.amount_swept, COALESCE(l.from_account,''), COALESCE(l.to_account,''),
		       COALESCE(l.status,''), COALESCE(l.error_message,''), l.balance_before, l.balance_after
		FROM cimplrcorpsaas.sweep_execution_log l
		LEFT JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = l.sweep_id
		WHERE l.execution_id = $1`, executionID,
	).Scan(&row.InitiationID, &row.SweepID, &row.EntityName,
		&row.ExecutionDate, &row.AmountSwept, &row.FromAccount, &row.ToAccount,
		&row.Status, &row.ErrorMessage, &row.BalanceBefore, &row.BalanceAfter)
	if err != nil {
		return sweepExecutionRow{}, fmt.Errorf("load sweep execution log for policy: %w", err)
	}
	return row, nil
}
