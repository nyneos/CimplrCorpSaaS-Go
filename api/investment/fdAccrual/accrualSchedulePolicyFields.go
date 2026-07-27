package fdAccrual

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fdAccrualScheduleRow is the canonical field set for FD_ACCRUAL_SCHED policy
// checks — the real scalar columns on investment.fd_accrual_schedule_config
// (jsonb notification_recipients excluded; created_at/updated_by/updated_at
// audit plumbing excluded per Database Modeling Rules, is_deleted excluded as
// pure soft-delete plumbing). Before this file: CreateScheduleConfig passed 5
// ad hoc fields, UpdateScheduleConfig passed only 3 (config_id/entity_id/
// entity_code — not even the fields actually being edited, despite the
// handler already loading old_* values for its own audit trail right below),
// DeleteScheduleConfig likewise passed only 3 while separately loading a full
// old-value snapshot it never fed to Enforce, and only Disable/Enable/
// Approve/Reject funnelled through accrualSchedulePolicyFields (7 fields).
// One struct, one builder, reused by all 7 live call sites in scheduler.go.
type fdAccrualScheduleRow struct {
	ConfigID              string
	EntityID              string
	EntityName            string
	ScheduleFrequency     string
	RunDayOfMonth         int
	RunTime               string
	DefaultBankIDFilter   string
	DefaultFDStatusFilter string
	DefaultRunMode        string
	AutoSubmitForApproval bool
	IsActive              bool
	LastRunAt             string
	LastRunID             string
	LastRunStatus         string
	NextRunAt             string
	CreatedBy             string
	AccrualGranularity    string
	PeriodCoverage        string
}

// buildFDAccrualSchedulePolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for FD_ACCRUAL_SCHED (see
// cmd/seedDomainCatalog/fdAccrualSchedCanonical.go).
func buildFDAccrualSchedulePolicyFields(row fdAccrualScheduleRow) map[string]interface{} {
	return map[string]interface{}{
		"config_id":                row.ConfigID,
		"entity_id":                row.EntityID,
		"entity_code":              row.EntityID,
		"entity_name":              row.EntityName,
		"schedule_frequency":       row.ScheduleFrequency,
		"run_day_of_month":         row.RunDayOfMonth,
		"run_time":                 row.RunTime,
		"default_bank_id_filter":   row.DefaultBankIDFilter,
		"default_fd_status_filter": row.DefaultFDStatusFilter,
		"default_run_mode":         row.DefaultRunMode,
		"auto_submit_for_approval": row.AutoSubmitForApproval,
		"is_active":                row.IsActive,
		"last_run_at":              row.LastRunAt,
		"last_run_id":              row.LastRunID,
		"last_run_status":          row.LastRunStatus,
		"next_run_at":              row.NextRunAt,
		"created_by":               row.CreatedBy,
		"accrual_granularity":      row.AccrualGranularity,
		"period_coverage":          row.PeriodCoverage,
	}
}

// loadFDAccrualScheduleRow fetches the full schedule config row by
// config_id — used by every call site that only receives a config_id
// (Disable/Enable/Approve/Reject/Delete), and by UpdateScheduleConfig as the
// base row that applyFDAccrualScheduleEdits then overlays.
func loadFDAccrualScheduleRow(ctx context.Context, pool *pgxpool.Pool, configID string) (fdAccrualScheduleRow, error) {
	var row fdAccrualScheduleRow
	row.ConfigID = configID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_id,''), COALESCE(entity_name,''),
		       COALESCE(schedule_frequency,''), COALESCE(run_day_of_month,0),
		       COALESCE(run_time::text,''),
		       COALESCE(default_bank_id_filter,''), COALESCE(default_fd_status_filter,''),
		       COALESCE(default_run_mode,''), COALESCE(auto_submit_for_approval,false),
		       COALESCE(is_active,false),
		       COALESCE(TO_CHAR(last_run_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
		       COALESCE(last_run_id,''), COALESCE(last_run_status,''),
		       COALESCE(TO_CHAR(next_run_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
		       COALESCE(created_by,''), COALESCE(accrual_granularity,''), COALESCE(period_coverage,'')
		FROM investment.fd_accrual_schedule_config
		WHERE config_id=$1 AND COALESCE(is_deleted,false)=false`, configID,
	).Scan(
		&row.EntityID, &row.EntityName,
		&row.ScheduleFrequency, &row.RunDayOfMonth,
		&row.RunTime,
		&row.DefaultBankIDFilter, &row.DefaultFDStatusFilter,
		&row.DefaultRunMode, &row.AutoSubmitForApproval,
		&row.IsActive,
		&row.LastRunAt, &row.LastRunID, &row.LastRunStatus,
		&row.NextRunAt,
		&row.CreatedBy, &row.AccrualGranularity, &row.PeriodCoverage,
	)
	if err != nil {
		return row, fmt.Errorf("load accrual schedule config for policy: %w", err)
	}
	return row, nil
}

// applyFDAccrualScheduleEdits overlays a partial edit map
// (UpdateScheduleConfig's req.Fields — arbitrary subset, whatever the user
// actually changed) onto an already-loaded canonical row, so the policy
// check sees the full row as-it-will-be-after-the-edit, not just the touched
// keys. Mirrors the allowlist in UpdateScheduleConfig itself
// (notification_recipients is jsonb — not part of the canonical row, skipped
// here same as elsewhere).
func applyFDAccrualScheduleEdits(row fdAccrualScheduleRow, edits map[string]interface{}) fdAccrualScheduleRow {
	str := func(v interface{}) (string, bool) {
		s, ok := v.(string)
		return s, ok
	}
	num := func(v interface{}) (int, bool) {
		switch n := v.(type) {
		case float64:
			return int(n), true
		case int:
			return n, true
		}
		return 0, false
	}
	boolean := func(v interface{}) (bool, bool) {
		b, ok := v.(bool)
		return b, ok
	}
	for k, v := range edits {
		switch k {
		case "schedule_frequency":
			if s, ok := str(v); ok {
				row.ScheduleFrequency = s
			}
		case "period_coverage":
			if s, ok := str(v); ok {
				row.PeriodCoverage = s
			}
		case "run_day_of_month":
			if n, ok := num(v); ok {
				row.RunDayOfMonth = n
			}
		case "run_time":
			if s, ok := str(v); ok {
				row.RunTime = s
			}
		case "default_bank_id_filter":
			if s, ok := str(v); ok {
				row.DefaultBankIDFilter = s
			}
		case "default_fd_status_filter":
			if s, ok := str(v); ok {
				row.DefaultFDStatusFilter = s
			}
		case "default_run_mode":
			if s, ok := str(v); ok {
				row.DefaultRunMode = s
			}
		case "auto_submit_for_approval":
			if b, ok := boolean(v); ok {
				row.AutoSubmitForApproval = b
			}
		case "accrual_granularity":
			if s, ok := str(v); ok {
				row.AccrualGranularity = s
			}
		}
	}
	return row
}
