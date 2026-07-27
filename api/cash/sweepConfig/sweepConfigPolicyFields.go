package sweepconfig

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// sweepConfigRow is the canonical business-field shape for SWEEP_CONFIG — one
// field per real scalar column on cimplrcorpsaas.sweepconfiguration (old_*
// audit-mirror columns and is_deleted excluded, not policy-comparable). Every
// policy-check call site in sweepConfigV2.go builds its Fields{} map from a
// value of this type instead of hand-picking its own ad hoc subset. Before
// this file: CreateSweepConfigurationV2 passed 7 of 13 real fields,
// BulkCreateSweepConfigurationV2 passed 5, UpdateSweepConfigurationV2 passed
// a raw unvalidated {sweep_id, fields} edit blob (the exact anti-pattern
// BANK_BALANCE was fixed for), and BulkApprove/BulkReject/BulkRequestDelete
// each passed only sweep_id.
type sweepConfigRow struct {
	SweepID            string
	EntityName         string
	SourceBankName     string
	SourceBankAccount  string
	TargetBankName     string
	TargetBankAccount  string
	SweepType          string
	Frequency          string
	EffectiveDate      string
	ExecutionTime      string
	BufferAmount       *float64
	SweepAmount        *float64
	RequiresInitiation bool
}

// buildSweepConfigPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for SWEEP_CONFIG (see
// cmd/seedDomainCatalog/sweepConfigCanonical.go). Note: domain_catalog also
// has a leftover "fields" field_code from the pre-fix raw-edit-blob pattern —
// it doesn't correspond to any real column and is deliberately not emitted
// here.
func buildSweepConfigPolicyFields(row sweepConfigRow) map[string]interface{} {
	return map[string]interface{}{
		"sweep_id":            row.SweepID,
		"entity_name":         row.EntityName,
		"source_bank_name":    row.SourceBankName,
		"source_bank_account": row.SourceBankAccount,
		"target_bank_name":    row.TargetBankName,
		"target_bank_account": row.TargetBankAccount,
		"sweep_type":          row.SweepType,
		"frequency":           row.Frequency,
		"effective_date":      row.EffectiveDate,
		"execution_time":      row.ExecutionTime,
		"buffer_amount":       row.BufferAmount,
		"sweep_amount":        row.SweepAmount,
		"requires_initiation": row.RequiresInitiation,
	}
}

// loadSweepConfigRow fetches the full canonical row by sweep_id — used by
// BulkApprove/BulkReject/BulkRequestDelete, which only ever receive a
// sweep_id in the request, never the business data itself.
func loadSweepConfigRow(ctx context.Context, pool *pgxpool.Pool, sweepID string) (sweepConfigRow, error) {
	var row sweepConfigRow
	row.SweepID = sweepID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_name,''), COALESCE(source_bank_name,''), COALESCE(source_bank_account,''),
		       COALESCE(target_bank_name,''), COALESCE(target_bank_account,''),
		       COALESCE(sweep_type,''), COALESCE(frequency,''),
		       COALESCE(effective_date::text,''), COALESCE(execution_time::text,''),
		       buffer_amount, sweep_amount, COALESCE(requires_initiation,true)
		FROM cimplrcorpsaas.sweepconfiguration WHERE sweep_id = $1`, sweepID,
	).Scan(&row.EntityName, &row.SourceBankName, &row.SourceBankAccount,
		&row.TargetBankName, &row.TargetBankAccount,
		&row.SweepType, &row.Frequency,
		&row.EffectiveDate, &row.ExecutionTime,
		&row.BufferAmount, &row.SweepAmount, &row.RequiresInitiation)
	if err != nil {
		return sweepConfigRow{}, fmt.Errorf("load sweep config for policy: %w", err)
	}
	return row, nil
}

// applySweepConfigEdits overlays a partial edit map (UpdateSweepConfigurationV2's
// req.Fields — arbitrary subset, whatever the user actually changed) onto an
// already-loaded canonical row, so the policy check sees the full row
// as-it-will-be-after-the-edit, not just the touched keys. Mirrors the
// validation/normalization the real UPDATE handler applies (sweep_type and
// frequency are upper-cased; effective_date is normalized via parseDate) —
// read from UpdateSweepConfigurationV2 in sweepConfigV2.go, not invented.
func applySweepConfigEdits(row sweepConfigRow, edits map[string]interface{}) sweepConfigRow {
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
		case "entity_name":
			if s, ok := str(v); ok {
				row.EntityName = strings.TrimSpace(s)
			}
		case "source_bank_name":
			if s, ok := str(v); ok {
				row.SourceBankName = strings.TrimSpace(s)
			}
		case "source_bank_account":
			if s, ok := str(v); ok {
				row.SourceBankAccount = strings.TrimSpace(s)
			}
		case "target_bank_name":
			if s, ok := str(v); ok {
				row.TargetBankName = strings.TrimSpace(s)
			}
		case "target_bank_account":
			if s, ok := str(v); ok {
				row.TargetBankAccount = strings.TrimSpace(s)
			}
		case "sweep_type":
			if s, ok := str(v); ok {
				row.SweepType = strings.ToUpper(strings.TrimSpace(s))
			}
		case "frequency":
			if s, ok := str(v); ok {
				row.Frequency = strings.ToUpper(strings.TrimSpace(s))
			}
		case "effective_date":
			s := fmt.Sprint(v)
			if t, err := parseDate(s); err == nil {
				row.EffectiveDate = t.Format(constants.DateFormat)
			} else {
				row.EffectiveDate = s
			}
		case "execution_time":
			if s, ok := str(v); ok {
				row.ExecutionTime = s
			}
		case "buffer_amount":
			if n, ok := num(v); ok {
				row.BufferAmount = n
			}
		case "sweep_amount":
			if n, ok := num(v); ok {
				row.SweepAmount = n
			}
		case "requires_initiation":
			if b, ok := v.(bool); ok {
				row.RequiresInitiation = b
			}
		}
	}
	return row
}
