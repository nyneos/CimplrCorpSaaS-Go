package dmsjobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	dashboardbuilder "CimplrCorpSaas/api/dash/dashboardBuilder"
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/domaincatalog"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// enrichDispatchPayload adds row count, full source rows, and scalars so
// notification templates and DMS email covers show real DB values.
func enrichDispatchPayload(ctx context.Context, pool *pgxpool.Pool, runID string, payload map[string]interface{}) {
	if payload == nil {
		return
	}
	rows, rowCount, err := refetchRunSourceRows(ctx, pool, runID)
	if err != nil {
		api.LogError("[DMS-DISPATCH] enrich payload run=%s: %v", runID, err)
		return
	}
	payload["RowCount"] = rowCount
	payload["RowsInScope"] = rowCount
	payload["rows_in_scope"] = rowCount

	normRows := normalizePayloadRows(rows)
	if len(normRows) > 0 {
		payload["Rows"] = normRows
		payload["SourceRows"] = normRows
		payload["PoolRows"] = normRows
		payload["rows"] = normRows
	}

	if rowCount == 0 {
		return
	}
	var first map[string]any
	if len(rows) > 0 {
		first = rows[0]
	}
	if first == nil {
		return
	}
	for _, row := range rows {
		for k, v := range row {
			if k == "" || v == nil {
				continue
			}
			if _, exists := payload[k]; !exists {
				payload[k] = normalizePayloadValue(v)
			}
			pascal := snakeToPascal(k)
			if pascal != "" {
				if _, exists := payload[pascal]; !exists {
					payload[pascal] = normalizePayloadValue(v)
				}
			}
		}
	}
}

func snakeToPascal(s string) string {
	parts := strings.Split(strings.TrimSpace(s), "_")
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, "")
}

// refetchRunSourceRows repeats the generation fetch for a completed run.
func refetchRunSourceRows(ctx context.Context, pool *pgxpool.Pool, runID string) ([]map[string]any, int, error) {
	var ruleID, versionID, triggerType string
	var windowStart, windowEnd *string
	var storedCount int
	err := pool.QueryRow(ctx, `
		SELECT gr.rule_id::text, gr.version_id::text, gr.trigger_type,
		       gr.window_start::text, gr.window_end::text,
		       COALESCE(gr.source_row_count, 0)
		FROM dms_svc.generation_run gr
		WHERE gr.run_id = $1::uuid`, runID,
	).Scan(&ruleID, &versionID, &triggerType, &windowStart, &windowEnd, &storedCount)
	if err != nil {
		return nil, 0, err
	}

	rule, err := loadRuleHeader(ctx, pool, ruleID)
	if err != nil {
		return nil, storedCount, err
	}
	version, err := loadRuleVersion(ctx, pool, versionID)
	if err != nil {
		return nil, storedCount, err
	}

	sourceKey, err := domaincatalog.ResolveSubModuleAlias(ctx, pool, rule.SubModuleCode, "DASHBOARD")
	if err != nil {
		return nil, storedCount, err
	}
	filters, err := loadFilters(ctx, pool, version.VersionID)
	if err != nil {
		return nil, storedCount, err
	}
	dashFilters := make([]dashboardbuilder.WidgetFilterRule, 0, len(filters)+2)
	if rule.EntityID != "" {
		dashFilters = append(dashFilters, dashboardbuilder.WidgetFilterRule{
			Field: "entity_id", Type: "text", Op: "=", Value: rule.EntityID, Conjunction: "AND",
		})
	}
	for _, f := range filters {
		dashFilters = append(dashFilters, dashboardbuilder.WidgetFilterRule{
			Field: f.Field, Type: f.FieldType, Op: normalizeDmsFilterOp(f.Op),
			Value: f.Value, Value2: f.Value2, Conjunction: f.Conjunction,
		})
	}

	sourceIDs, sourceIDField, err := loadRunSourceIDs(ctx, pool, runID)
	if err != nil {
		return nil, storedCount, err
	}
	if strings.TrimSpace(sourceIDField) != "" && len(sourceIDs) > 0 {
		dashFilters = append(dashFilters, dashboardbuilder.WidgetFilterRule{
			Field: sourceIDField, Type: "id", Op: "in",
			Value: strings.Join(sourceIDs, ","), Conjunction: "AND",
		})
	}

	var entityIDs []string
	if rule.EntityID != "" {
		entityIDs = []string{rule.EntityID}
	}
	asOf, asOn := "", ""
	if windowStart != nil {
		asOf = strings.TrimSpace(*windowStart)
	}
	if windowEnd != nil {
		asOn = strings.TrimSpace(*windowEnd)
	}
	bankScope, err := loadBankAccountScope(ctx, pool, version.VersionID)
	if err != nil {
		return nil, storedCount, err
	}
	rowFrom, rowTo := normalizeDataRowRange(version.DataRowFrom, version.DataRowTo)
	rows, _, err := dashboardbuilder.FetchSourceData(ctx, pool, dashboardbuilder.DataRequest{
		Source: sourceKey, EntityIDs: entityIDs, Filters: dashFilters,
		Limit: rowTo - rowFrom + 1, Offset: rowFrom - 1,
		AsOfDate: asOf, AsOnDate: asOn,
		BankAccountScope: bankScope, AllowUnscopedBankAccount: len(bankScope) == 0,
		EnforceDateWindow: true,
	})
	if err != nil {
		return nil, storedCount, fmt.Errorf("refetch: %w", err)
	}
	return rows, len(rows), nil
}

func loadRunSourceIDs(ctx context.Context, pool *pgxpool.Pool, runID string) ([]string, string, error) {
	var sourceIDField string
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(t.source_id_field, '')
		FROM dms_svc.generation_run gr
		JOIN dms_svc.generation_rule r ON r.rule_id = gr.rule_id
		JOIN dms_svc.generation_rule_trigger t ON t.version_id = gr.version_id AND t.is_enabled
		WHERE gr.run_id = $1::uuid
		ORDER BY t.sort_order LIMIT 1`, runID).Scan(&sourceIDField)

	rows, err := pool.Query(ctx, `
		SELECT source_id FROM dms_svc.generation_run_source_row
		WHERE run_id = $1::uuid ORDER BY sort_order`, runID)
	if err != nil {
		return nil, sourceIDField, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, sourceIDField, err
		}
		ids = append(ids, id)
	}
	return ids, sourceIDField, rows.Err()
}

func normalizePayloadRows(rows []map[string]any) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		norm := make(map[string]interface{}, len(row))
		for k, v := range row {
			norm[k] = normalizePayloadValue(v)
		}
		out = append(out, norm)
	}
	return out
}

func normalizePayloadValue(v any) any {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case time.Time:
		return t.Format("2006-01-02")
	case *time.Time:
		if t == nil {
			return nil
		}
		return t.Format("2006-01-02")
	case pgtype.Numeric:
		if !t.Valid {
			return nil
		}
		if fv, err := t.Float64Value(); err == nil {
			return fv.Float64
		}
		return nil
	case *pgtype.Numeric:
		if t == nil || !t.Valid {
			return nil
		}
		if fv, err := t.Float64Value(); err == nil {
			return fv.Float64
		}
		return nil
	case float64, float32, int, int32, int64, bool, string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(v)
	}
}
