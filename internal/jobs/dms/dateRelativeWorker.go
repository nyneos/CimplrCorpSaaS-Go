package dmsjobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	dashboardbuilder "CimplrCorpSaas/api/dash/dashboardBuilder"
	"CimplrCorpSaas/api/domaincatalog"
	"CimplrCorpSaas/internal/config"
	"CimplrCorpSaas/internal/services/docsvc"

	"github.com/jackc/pgx/v5/pgxpool"
)

type dateRelativeRule struct {
	RuleID        string
	SubModuleCode string
	DateField     string
	SourceIDField string
	OffsetDays    int
	DataRowFrom   int
	DataRowTo     int
}

// StartDateRelativeWorker resolves reminders such as FD maturity T-3.
// It polls hourly but de-duplicates each (rule, source row) per local day.
func StartDateRelativeWorker(ctx context.Context, pool *pgxpool.Pool) {
	if pool == nil {
		return
	}
	loc, err := time.LoadLocation(config.DefaultTimeZone)
	if err != nil {
		api.LogError("[DMS-DATE] invalid timezone %q: %v", config.DefaultTimeZone, err)
		return
	}
	run := func() {
		if err := pollDateRelativeRules(ctx, pool, time.Now().In(loc), loc); err != nil {
			api.LogError("[DMS-DATE] poll: %v", err)
		}
	}
	run()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func pollDateRelativeRules(
	ctx context.Context,
	pool *pgxpool.Pool,
	now time.Time,
	loc *time.Location,
) error {
	rows, err := pool.Query(ctx, `
		SELECT r.rule_id::text, r.sub_module_code, t.date_field,
		       t.source_id_field, COALESCE(t.offset_days, 0),
		       COALESCE(v.data_row_from, 1), COALESCE(v.data_row_to, 500)
		FROM dms_svc.generation_rule r
		JOIN dms_svc.generation_rule_version v ON v.version_id = r.current_version_id
		JOIN dms_svc.generation_rule_trigger t ON t.version_id = v.version_id
		WHERE r.status = 'Active'
		  AND r.processing_status = 'APPROVED'
		  AND r.is_deleted = false
		  AND v.status = 'APPROVED'
		  AND t.trigger_type = 'DATE_RELATIVE'
		  AND t.is_enabled = true`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var rules []dateRelativeRule
	for rows.Next() {
		var rule dateRelativeRule
		if err := rows.Scan(
			&rule.RuleID, &rule.SubModuleCode, &rule.DateField,
			&rule.SourceIDField, &rule.OffsetDays,
			&rule.DataRowFrom, &rule.DataRowTo,
		); err != nil {
			return err
		}
		rules = append(rules, rule)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	dayEnd := dayStart.AddDate(0, 0, 1)
	for _, rule := range rules {
		if strings.TrimSpace(rule.DateField) == "" || strings.TrimSpace(rule.SourceIDField) == "" {
			continue
		}
		sourceKey, err := domaincatalog.ResolveSubModuleAlias(ctx, pool, rule.SubModuleCode, "DASHBOARD")
		if err != nil {
			api.LogError("[DMS-DATE] rule=%s resolve source: %v", rule.RuleID, err)
			continue
		}
		// fire_date = source_date + offset; therefore source_date = today - offset.
		targetDate := now.AddDate(0, 0, -rule.OffsetDays).Format("2006-01-02")
		rowFrom, rowTo := normalizeDataRowRange(rule.DataRowFrom, rule.DataRowTo)
		fetchLimit := rowTo - rowFrom + 1
		fetchOffset := rowFrom - 1
		sourceRows, _, err := dashboardbuilder.FetchSourceData(ctx, pool, dashboardbuilder.DataRequest{
			Source: sourceKey,
			Limit:  fetchLimit,
			Offset: fetchOffset,
			Filters: []dashboardbuilder.WidgetFilterRule{{
				Field: rule.DateField, Type: "date", Op: "=", Value: targetDate,
			}},
			AllowUnscopedBankAccount: true,
		})
		if err != nil {
			api.LogError("[DMS-DATE] rule=%s fetch target=%s: %v", rule.RuleID, targetDate, err)
			continue
		}
		var sourceIDs []string
		for _, sourceRow := range sourceRows {
			sourceID := strings.TrimSpace(formatFieldValue(sourceRow[rule.SourceIDField]))
			if sourceID == "" || sourceID == "-" {
				continue
			}
			var exists bool
			if err := pool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM dms_svc.generation_run gr
					JOIN dms_svc.generation_run_source_row sr ON sr.run_id = gr.run_id
					WHERE gr.rule_id = $1::uuid
					  AND gr.trigger_type = 'DATE_RELATIVE'
					  AND sr.source_id = $2
					  AND gr.started_at >= $3
					  AND gr.started_at < $4
				)`, rule.RuleID, sourceID, dayStart, dayEnd).Scan(&exists); err != nil {
				return fmt.Errorf("date trigger dedupe: %w", err)
			}
			if !exists {
				sourceIDs = append(sourceIDs, sourceID)
			}
		}
		if len(sourceIDs) == 0 {
			continue
		}
		if docsvc.QueueEnabled() {
			jobID, qErr := EnqueueRuleGeneration(
				ctx, pool, rule.RuleID, "DATE_RELATIVE", "SYSTEM:dms-date-trigger",
				rule.SourceIDField, sourceIDs,
			)
			if qErr != nil {
				api.LogError("[DMS-DATE] rule=%s enqueue: %v", rule.RuleID, qErr)
			} else {
				api.LogInfo("[DMS-DATE] rule=%s job=%s queued source_rows=%d", rule.RuleID, jobID, len(sourceIDs))
			}
			continue
		}
		runID, runErr := RunGenerationForSourceIDs(
			ctx, pool, rule.RuleID, "DATE_RELATIVE", "SYSTEM:dms-date-trigger",
			rule.SourceIDField, sourceIDs,
		)
		if runErr != nil {
			api.LogError("[DMS-DATE] rule=%s run=%s: %v", rule.RuleID, runID, runErr)
		}
	}
	return nil
}
