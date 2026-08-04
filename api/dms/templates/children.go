package templates

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// insertVersionChildren attaches merge fields and chart placeholders (with
// their filters) to a freshly inserted dms_svc.template_version row.
func insertVersionChildren(ctx context.Context, tx pgx.Tx, versionID string, mergeFields []mergeFieldReq, placeholders []chartPlaceholderReq) error {
	for i, mf := range mergeFields {
		key := strings.TrimSpace(mf.FieldKey)
		fieldID := strings.TrimSpace(mf.DomainCatalogFieldID)
		if key == "" || fieldID == "" {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dms_svc.template_merge_field (version_id, field_key, domain_catalog_field_id, display_format, sort_order)
			VALUES ($1::uuid, $2, $3::uuid, $4, $5)`,
			versionID, key, fieldID, mf.DisplayFormat, i); err != nil {
			return fmt.Errorf("merge field %q: %w", key, err)
		}
	}

	for i, ph := range placeholders {
		key := strings.TrimSpace(ph.PlaceholderKey)
		if key == "" {
			continue
		}
		var placeholderID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO dms_svc.template_chart_placeholder
				(version_id, placeholder_key, chart_type, data_source, dimension_field, measure_field, sort_order)
			VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)
			RETURNING placeholder_id`,
			versionID, key, ph.ChartType, ph.DataSource, ph.DimensionField, ph.MeasureField, i,
		).Scan(&placeholderID); err != nil {
			return fmt.Errorf("chart placeholder %q: %w", key, err)
		}
		for j, f := range ph.Filters {
			conjunction := strings.TrimSpace(strings.ToUpper(f.Conjunction))
			if conjunction == "" {
				conjunction = "AND"
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO dms_svc.template_chart_filter
					(placeholder_id, field, op, value, value2, conjunction, sort_order)
				VALUES ($1::uuid, $2, $3, NULLIF($4,''), NULLIF($5,''), $6, $7)`,
				placeholderID, f.Field, f.Op, f.Value, f.Value2, conjunction, j); err != nil {
				return fmt.Errorf("chart placeholder %q filter %d: %w", key, j, err)
			}
		}
	}
	return nil
}

// loadVersionChildren reads the merge fields and chart placeholders/filters
// for a single version — used by HandleDetail.
func loadVersionChildren(ctx context.Context, tx pgx.Tx, versionID string) ([]mergeFieldReq, []chartPlaceholderReq, error) {
	mergeFields := make([]mergeFieldReq, 0)
	mfRows, err := tx.Query(ctx, `
		SELECT field_key, domain_catalog_field_id::text, display_format
		FROM dms_svc.template_merge_field
		WHERE version_id = $1::uuid ORDER BY sort_order`, versionID)
	if err != nil {
		return nil, nil, err
	}
	for mfRows.Next() {
		var mf mergeFieldReq
		if err := mfRows.Scan(&mf.FieldKey, &mf.DomainCatalogFieldID, &mf.DisplayFormat); err != nil {
			mfRows.Close()
			return nil, nil, err
		}
		mergeFields = append(mergeFields, mf)
	}
	mfRows.Close()

	placeholders := make([]chartPlaceholderReq, 0)
	phRows, err := tx.Query(ctx, `
		SELECT placeholder_id::text, placeholder_key, chart_type, data_source, dimension_field, measure_field
		FROM dms_svc.template_chart_placeholder
		WHERE version_id = $1::uuid ORDER BY sort_order`, versionID)
	if err != nil {
		return nil, nil, err
	}
	type phRow struct {
		id string
		ph chartPlaceholderReq
	}
	var phs []phRow
	for phRows.Next() {
		var pid string
		var ph chartPlaceholderReq
		if err := phRows.Scan(&pid, &ph.PlaceholderKey, &ph.ChartType, &ph.DataSource, &ph.DimensionField, &ph.MeasureField); err != nil {
			phRows.Close()
			return nil, nil, err
		}
		phs = append(phs, phRow{id: pid, ph: ph})
	}
	phRows.Close()

	for i := range phs {
		filterRows, err := tx.Query(ctx, `
			SELECT field, op, COALESCE(value,''), COALESCE(value2,''), conjunction
			FROM dms_svc.template_chart_filter
			WHERE placeholder_id = $1::uuid ORDER BY sort_order`, phs[i].id)
		if err != nil {
			return nil, nil, err
		}
		var filters []chartFilterReq
		for filterRows.Next() {
			var f chartFilterReq
			if err := filterRows.Scan(&f.Field, &f.Op, &f.Value, &f.Value2, &f.Conjunction); err != nil {
				filterRows.Close()
				return nil, nil, err
			}
			filters = append(filters, f)
		}
		filterRows.Close()
		phs[i].ph.Filters = filters
		placeholders = append(placeholders, phs[i].ph)
	}
	return mergeFields, placeholders, nil
}
