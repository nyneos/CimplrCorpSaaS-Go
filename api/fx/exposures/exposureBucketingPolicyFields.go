package exposures

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// exposureEntityForHeader resolves the owning entity for an exposure header.
// exposure_bucketing has no entity column of its own, and passing the header
// ID as EntityCode (as these handlers used to) never matches an entity scope —
// so an include list silently skips the policy and an exclude list stops
// excluding. Empty on failure, which is the same as supplying nothing.
func exposureEntityForHeader(ctx context.Context, pool *pgxpool.Pool, exposureHeaderID string) string {
	if strings.TrimSpace(exposureHeaderID) == "" || pool == nil {
		return ""
	}
	row, err := LoadExposureCreationRow(ctx, pool, exposureHeaderID)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(row.Entity)
}

// exposureBucketingRow is the canonical business-field shape for
// EXPOSURE_BUCKETING — one field per real scalar column on
// public.exposure_bucketing (created_at skipped as pure audit plumbing).
// EXPOSURE_BUCKETING is a rollout gap, not a consistency bug: before this
// file, none of the four real mutation handlers in expBucketing.go
// (UpdateExposureHeadersLineItemsBucketing, DeleteBucketingStatus,
// ApproveBucketingStatus, RejectBucketingStatus) called runtime.Enforce at
// all — see database/2026-07-27.sql.
//
// The domain_catalog.field seed already carries a "status" field_code for
// this sub-module with no matching real column; StatusBucketing backs both
// "status" and "status_bucketing" (same alias pattern fdMasterRow uses for
// entity_id/entity_code) rather than inventing a column that doesn't exist.
type exposureBucketingRow struct {
	ExposureHeaderID string
	ReferenceNo      string
	Date             string
	Time             string
	Advance          float64
	Month1           float64
	Month2           float64
	Month3           float64
	Month4           float64
	Month4to6        float64
	Month6Plus       float64
	OldMonth1        float64
	OldMonth2        float64
	OldMonth3        float64
	OldMonth4        float64
	OldMonth4to6     float64
	OldMonth6Plus    float64
	StatusBucketing  string
	Comments         string
	UpdatedBy        string
	UpdatedAt        string
}

// buildExposureBucketingPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for EXPOSURE_BUCKETING (see
// cmd/seedDomainCatalog/exposureBucketingCanonical.go).
func buildExposureBucketingPolicyFields(row exposureBucketingRow) map[string]interface{} {
	return map[string]interface{}{
		"exposure_header_id": row.ExposureHeaderID,
		"reference_no":       row.ReferenceNo,
		"date":               row.Date,
		"time":               row.Time,
		"advance":            row.Advance,
		"month_1":            row.Month1,
		"month_2":            row.Month2,
		"month_3":            row.Month3,
		"month_4":            row.Month4,
		"month_4_6":          row.Month4to6,
		"month_6plus":        row.Month6Plus,
		"old_month1":         row.OldMonth1,
		"old_month2":         row.OldMonth2,
		"old_month3":         row.OldMonth3,
		"old_month4":         row.OldMonth4,
		"old_month4to6":      row.OldMonth4to6,
		"old_month6plus":     row.OldMonth6Plus,
		"status":             row.StatusBucketing,
		"status_bucketing":   row.StatusBucketing,
		"comments":           row.Comments,
		"updated_by":         row.UpdatedBy,
		"updated_at":         row.UpdatedAt,
	}
}

// loadExposureBucketingRow fetches the full canonical row by
// exposure_header_id — used by DeleteBucketingStatus/ApproveBucketingStatus/
// RejectBucketingStatus, which only ever receive exposure_header_id(s), never
// the full bucketing row.
func loadExposureBucketingRow(ctx context.Context, pool *pgxpool.Pool, exposureHeaderID string) (exposureBucketingRow, error) {
	var row exposureBucketingRow
	row.ExposureHeaderID = exposureHeaderID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(reference_no,''), COALESCE(date::text,''), COALESCE(time::text,''),
		       COALESCE(advance,0), COALESCE(month_1,0), COALESCE(month_2,0), COALESCE(month_3,0),
		       COALESCE(month_4,0), COALESCE(month_4_6,0), COALESCE(month_6plus,0),
		       COALESCE(old_month1,0), COALESCE(old_month2,0), COALESCE(old_month3,0),
		       COALESCE(old_month4,0), COALESCE(old_month4to6,0), COALESCE(old_month6plus,0),
		       COALESCE(status_bucketing,''), COALESCE(comments,''), COALESCE(updated_by,''), COALESCE(updated_at::text,'')
		FROM exposure_bucketing
		WHERE exposure_header_id = $1`, exposureHeaderID,
	).Scan(
		&row.ReferenceNo, &row.Date, &row.Time,
		&row.Advance, &row.Month1, &row.Month2, &row.Month3,
		&row.Month4, &row.Month4to6, &row.Month6Plus,
		&row.OldMonth1, &row.OldMonth2, &row.OldMonth3,
		&row.OldMonth4, &row.OldMonth4to6, &row.OldMonth6Plus,
		&row.StatusBucketing, &row.Comments, &row.UpdatedBy, &row.UpdatedAt,
	)
	if err != nil {
		return row, fmt.Errorf("load exposure bucketing row for policy: %w", err)
	}
	return row, nil
}

// applyExposureBucketingEdits overlays a partial edit map (from
// UpdateExposureHeadersLineItemsBucketing's req.BucketingFields, already
// column-filtered by allowedExposureBucketingCols, including the
// status_bucketing='PENDING_EDIT_APPROVAL' transition the handler forces
// onto every edit) onto an already-loaded canonical row, so the policy check
// sees the row as it will read after the edit.
func applyExposureBucketingEdits(row exposureBucketingRow, edits map[string]interface{}) exposureBucketingRow {
	str := func(v interface{}) (string, bool) {
		s, ok := v.(string)
		return s, ok
	}
	num := func(v interface{}) (float64, bool) {
		switch n := v.(type) {
		case float64:
			return n, true
		case int:
			return float64(n), true
		}
		return 0, false
	}
	for k, v := range edits {
		switch k {
		case "status_bucketing":
			if s, ok := str(v); ok {
				row.StatusBucketing = s
			}
		case "comments":
			if s, ok := str(v); ok {
				row.Comments = s
			}
		case "updated_by":
			if s, ok := str(v); ok {
				row.UpdatedBy = s
			}
		case "month_1":
			if n, ok := num(v); ok {
				row.Month1 = n
			}
		case "month_2":
			if n, ok := num(v); ok {
				row.Month2 = n
			}
		case "month_3":
			if n, ok := num(v); ok {
				row.Month3 = n
			}
		case "month_4":
			if n, ok := num(v); ok {
				row.Month4 = n
			}
		case "month_4_6":
			if n, ok := num(v); ok {
				row.Month4to6 = n
			}
		case "month_6plus":
			if n, ok := num(v); ok {
				row.Month6Plus = n
			}
		case "old_month1":
			if n, ok := num(v); ok {
				row.OldMonth1 = n
			}
		case "old_month2":
			if n, ok := num(v); ok {
				row.OldMonth2 = n
			}
		case "old_month3":
			if n, ok := num(v); ok {
				row.OldMonth3 = n
			}
		case "old_month4":
			if n, ok := num(v); ok {
				row.OldMonth4 = n
			}
		case "old_month4to6":
			if n, ok := num(v); ok {
				row.OldMonth4to6 = n
			}
		case "old_month6plus":
			if n, ok := num(v); ok {
				row.OldMonth6Plus = n
			}
		case "advance":
			if n, ok := num(v); ok {
				row.Advance = n
			}
		}
	}
	return row
}
