package sweepconfig

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// sweepInitiationRow is the canonical business-field shape for
// SWEEP_INITIATION — the real scalar columns on
// cimplrcorpsaas.sweep_initiation (is_deleted excluded, not
// policy-comparable), plus entity_name/source_bank_account/
// target_bank_account/sweep_type/buffer_amount/sweep_amount, which are not
// real columns on sweep_initiation itself but are denormalized from the
// parent cimplrcorpsaas.sweepconfiguration row — this enrichment pattern was
// already established in domain_catalog before this fix (entity_name,
// source_bank_account, target_bank_account, sweep_type, buffer_amount were
// already seeded as SWEEP_INITIATION fields) and is what
// CreateSweepInitiation's existing Fields maps were built from. ProcessingStatus
// is the latest cimplrcorpsaas.auditactionsweepinitiation.processing_status.
// AutoCreateSweep reflects the request-time "sweep_id was empty, so a sweep
// was auto-created" branch — it is not stored anywhere in the DB, so
// loadSweepInitiationRow (used for actions that only receive an
// initiation_id, i.e. after the sweep already exists) always resolves it to
// false; only CreateSweepInitiation/BulkCreateSweepInitiation, which know
// whether they took the auto-create branch, set it to true. Flagged for
// reviewer: this is a judgment call, not a stored fact.
type sweepInitiationRow struct {
	InitiationID                string
	SweepID                     string
	InitiatedBy                 string
	InitiationTime              string
	OverriddenAmount            *float64
	OverriddenExecutionTime     string
	OverriddenSourceBankAccount string
	OverriddenTargetBankAccount string
	// enriched from parent cimplrcorpsaas.sweepconfiguration
	EntityName        string
	SourceBankAccount string
	TargetBankAccount string
	SweepType         string
	BufferAmount      *float64
	SweepAmount       *float64
	// synthetic / contextual
	AutoCreateSweep  bool
	ProcessingStatus string
}

// derefStr safely dereferences an optional string pointer (request payloads
// use *string for "explicitly unset" vs "explicitly empty" distinctions that
// don't matter for the policy field set).
func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// buildSweepInitiationPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for SWEEP_INITIATION (see
// cmd/seedDomainCatalog/sweepInitiationCanonical.go). overridden_source_bank_account,
// overridden_target_bank_account, and sweep_amount are newly added by that
// seed file — the first two are real columns on sweep_initiation that had no
// domain_catalog row at all; sweep_amount was already being passed ad hoc by
// CreateSweepInitiation's Fields map (as a sibling to the already-seeded
// buffer_amount enrichment field) without ever being cataloged.
func buildSweepInitiationPolicyFields(row sweepInitiationRow) map[string]interface{} {
	return map[string]interface{}{
		"initiation_id":                  row.InitiationID,
		"sweep_id":                       row.SweepID,
		"initiated_by":                   row.InitiatedBy,
		"initiation_time":                row.InitiationTime,
		"overridden_amount":              row.OverriddenAmount,
		"overridden_execution_time":      row.OverriddenExecutionTime,
		"overridden_source_bank_account": row.OverriddenSourceBankAccount,
		"overridden_target_bank_account": row.OverriddenTargetBankAccount,
		"entity_name":                    row.EntityName,
		"source_bank_account":            row.SourceBankAccount,
		"target_bank_account":            row.TargetBankAccount,
		"sweep_type":                     row.SweepType,
		"buffer_amount":                  row.BufferAmount,
		"sweep_amount":                   row.SweepAmount,
		"auto_create_sweep":              row.AutoCreateSweep,
		"processing_status":              row.ProcessingStatus,
	}
}

// loadSweepInitiationRow fetches the full canonical row by initiation_id —
// used by BulkApprove/BulkReject/BulkDelete/UpdateSweepInitiation, which only
// ever receive an initiation_id in the request, never the business data
// itself. Joins to the parent sweepconfiguration for the enrichment fields
// and to the latest auditactionsweepinitiation row for processing_status.
func loadSweepInitiationRow(ctx context.Context, pool *pgxpool.Pool, initiationID string) (sweepInitiationRow, error) {
	var row sweepInitiationRow
	row.InitiationID = initiationID
	err := pool.QueryRow(ctx, `
		SELECT si.sweep_id, COALESCE(si.initiated_by,''), COALESCE(si.initiation_time::text,''),
		       si.overridden_amount, COALESCE(si.overridden_execution_time::text,''),
		       COALESCE(si.overridden_source_bank_account,''), COALESCE(si.overridden_target_bank_account,''),
		       COALESCE(sc.entity_name,''), COALESCE(sc.source_bank_account,''), COALESCE(sc.target_bank_account,''),
		       COALESCE(sc.sweep_type,''), sc.buffer_amount, sc.sweep_amount
		FROM cimplrcorpsaas.sweep_initiation si
		LEFT JOIN cimplrcorpsaas.sweepconfiguration sc ON sc.sweep_id = si.sweep_id
		WHERE si.initiation_id = $1`, initiationID,
	).Scan(&row.SweepID, &row.InitiatedBy, &row.InitiationTime,
		&row.OverriddenAmount, &row.OverriddenExecutionTime,
		&row.OverriddenSourceBankAccount, &row.OverriddenTargetBankAccount,
		&row.EntityName, &row.SourceBankAccount, &row.TargetBankAccount,
		&row.SweepType, &row.BufferAmount, &row.SweepAmount)
	if err != nil {
		return sweepInitiationRow{}, fmt.Errorf("load sweep initiation for policy: %w", err)
	}

	var status *string
	_ = pool.QueryRow(ctx, `
		SELECT processing_status FROM cimplrcorpsaas.auditactionsweepinitiation
		WHERE initiation_id = $1 ORDER BY requested_at DESC, action_id DESC LIMIT 1`, initiationID,
	).Scan(&status)
	if status != nil {
		row.ProcessingStatus = *status
	}

	return row, nil
}

// applySweepInitiationEdits overlays a partial edit map (UpdateSweepInitiation's
// request fields — arbitrary subset, whatever the user actually changed) onto
// an already-loaded canonical row, so the policy check sees the row
// as-it-will-be-after-the-edit, not just the touched keys.
//
// UpdateSweepInitiation is unusual among this sub-module's handlers: a single
// call can edit fields on *both* tables — sweep_initiation's own
// overridden_* columns, and (per real code in sweepInitiationV2.go)
// sweep_type/buffer_amount/sweep_amount on the parent sweepconfiguration row.
// Both are represented here since both are already in the field vocabulary.
//
// Flagged for reviewer: the real UpdateSweepInitiation handler can *also*
// edit sweepconfiguration's frequency/effective_date/execution_time via the
// same call, but none of those three are seeded as SWEEP_INITIATION fields
// (they belong to SWEEP_CONFIG's vocabulary) — adding them here would mean
// inventing new field_codes beyond what CreateSweepInitiation's existing
// (fixed) Fields map already established, so they are deliberately left out
// and not editable through this function. A policy scoped to SWEEP_INITIATION
// cannot currently gate on a frequency/effective_date/execution_time change
// made through this endpoint.
func applySweepInitiationEdits(row sweepInitiationRow, edits map[string]interface{}) sweepInitiationRow {
	str := func(v interface{}) (string, bool) {
		s, ok := v.(string)
		return s, ok
	}
	num := func(v interface{}) (*float64, bool) {
		switch n := v.(type) {
		case float64:
			return &n, true
		case int:
			f := float64(n)
			return &f, true
		}
		return nil, false
	}
	for k, v := range edits {
		switch k {
		case "overridden_amount":
			if n, ok := num(v); ok {
				row.OverriddenAmount = n
			}
		case "overridden_execution_time":
			if s, ok := str(v); ok {
				row.OverriddenExecutionTime = s
			}
		case "overridden_source_bank_account":
			if s, ok := str(v); ok {
				row.OverriddenSourceBankAccount = strings.TrimSpace(s)
			}
		case "overridden_target_bank_account":
			if s, ok := str(v); ok {
				row.OverriddenTargetBankAccount = strings.TrimSpace(s)
			}
		case "sweep_type":
			if s, ok := str(v); ok {
				row.SweepType = strings.ToUpper(strings.TrimSpace(s))
			}
		case "buffer_amount":
			if n, ok := num(v); ok {
				row.BufferAmount = n
			}
		case "sweep_amount":
			if n, ok := num(v); ok {
				row.SweepAmount = n
			}
		}
	}
	return row
}
