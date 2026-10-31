package dmsjobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/config"
	"CimplrCorpSaas/internal/services/docsvc"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

const (
	scheduleWorkerPoll   = 60 * time.Second
	scheduleTriggeredBy  = "SYSTEM:dms-scheduler"
	scheduleCronSpecBits = cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor
	dmsRuleRunTimeout = 10 * time.Minute
	dateRelativeActor = "SYSTEM:dms-date-trigger"
)

type cronRuleCandidate struct {
	RuleID   string
	CronExpr string
}

// StartScheduleWorker polls approved CRON generation rules and triggers
// RunGeneration when a cron tick falls inside the last poll window.
func StartScheduleWorker(ctx context.Context, pool *pgxpool.Pool) {
	if pool == nil {
		return
	}

	loc, err := time.LoadLocation(config.DefaultTimeZone)
	if err != nil {
		api.LogError("[DMS-SCHEDULE] invalid timezone %q: %v", config.DefaultTimeZone, err)
		return
	}

	parser := cron.NewParser(scheduleCronSpecBits)
	api.LogInfo("[DMS-SCHEDULE] worker started (poll=%s tz=%s)", scheduleWorkerPoll, loc)

	ticker := time.NewTicker(scheduleWorkerPoll)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			api.LogInfo("[DMS-SCHEDULE] worker stopped")
			return
		case <-ticker.C:
			pollScheduledRules(ctx, pool, parser, loc)
		}
	}
}

func pollScheduledRules(ctx context.Context, pool *pgxpool.Pool, parser cron.Parser, loc *time.Location) {
	rules, err := loadCronRules(ctx, pool)
	if err != nil {
		api.LogError("[DMS-SCHEDULE] load CRON rules: %v", err)
		return
	}
	if len(rules) == 0 {
		return
	}

	now := time.Now().In(loc)
	for _, rule := range rules {
		schedule, err := parser.Parse(strings.TrimSpace(rule.CronExpr))
		if err != nil {
			api.LogError("[DMS-SCHEDULE] rule=%s invalid cron_expr %q: %v", rule.RuleID, rule.CronExpr, err)
			continue
		}

		tick, due := cronTickDue(schedule, now, scheduleWorkerPoll)
		if !due {
			continue
		}

		runID, jobID, claimed, err := reserveScheduledTick(ctx, pool, rule.RuleID, tick)
		if err != nil {
			api.LogError("[DMS-SCHEDULE] rule=%s tick=%s reserve: %v", rule.RuleID, tick.Format(time.RFC3339), err)
			continue
		}
		if !claimed {
			continue
		}

		if docsvc.QueueEnabled() {
			api.LogInfo("[DMS-SCHEDULE] rule=%s tick=%s job=%s run=%s queued", rule.RuleID, tick.Format(time.RFC3339), jobID, runID)
			continue
		}

		ruleCtx, cancel := context.WithTimeout(ctx, dmsRuleRunTimeout)
		finishedID, runErr := runGeneration(ruleCtx, pool, rule.RuleID, "SCHEDULED", scheduleTriggeredBy, "", nil, runID)
		cancel()
		if runErr != nil {
			api.LogError("[DMS-SCHEDULE] rule=%s tick=%s run=%s: %v", rule.RuleID, tick.Format(time.RFC3339), finishedID, runErr)
			continue
		}
		api.LogInfo("[DMS-SCHEDULE] rule=%s tick=%s run=%s triggered", rule.RuleID, tick.Format(time.RFC3339), finishedID)
	}
}

func loadCronRules(ctx context.Context, pool *pgxpool.Pool) ([]cronRuleCandidate, error) {
	rows, err := pool.Query(ctx, `
		SELECT r.rule_id::text, TRIM(v.cron_expr)
		FROM dms_svc.generation_rule r
		JOIN dms_svc.generation_rule_version v ON v.version_id = r.current_version_id
		WHERE r.is_deleted = false
		  AND r.processing_status = 'APPROVED'
		  AND r.status = 'Active'
		  AND r.current_version_id IS NOT NULL
		  AND v.status = 'APPROVED'
		  AND v.schedule_type = 'CRON'
		  AND v.cron_expr IS NOT NULL
		  AND TRIM(v.cron_expr) <> ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []cronRuleCandidate
	for rows.Next() {
		var c cronRuleCandidate
		if err := rows.Scan(&c.RuleID, &c.CronExpr); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// cronTickDue returns the scheduled instant when the cron expression fired
// within (now - pollInterval, now], if any.
func cronTickDue(schedule cron.Schedule, now time.Time, pollInterval time.Duration) (time.Time, bool) {
	windowStart := now.Add(-pollInterval)
	tick := schedule.Next(windowStart.Add(-time.Second))
	if tick.After(now) || tick.Before(windowStart) {
		return time.Time{}, false
	}
	return tick, true
}

func ruleTickLockName(kind, ruleID string, tick time.Time) string {
	window := tick.Truncate(time.Minute).Format(time.RFC3339)
	return fmt.Sprintf("dms-%s:%s:%s", kind, strings.TrimSpace(ruleID), window)
}

func withRuleTickLock(ctx context.Context, pool *pgxpool.Pool, lockName string, fn func(context.Context, pgx.Tx) error) (bool, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire tick lock %s: %w", lockName, err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tick lock %s: %w", lockName, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var acquired bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext($1))`, lockName).Scan(&acquired); err != nil {
		return false, fmt.Errorf("pg_try_advisory_xact_lock %s: %w", lockName, err)
	}
	if !acquired {
		api.LogInfo("[DMS] tick lock %s already held — skipping", lockName)
		return false, nil
	}
	if err := fn(ctx, tx); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit tick lock %s: %w", lockName, err)
	}
	return true, nil
}

func reserveScheduledTick(ctx context.Context, pool *pgxpool.Pool, ruleID string, tick time.Time) (runID, jobID string, claimed bool, err error) {
	lockName := ruleTickLockName("schedule", ruleID, tick)
	var reservedRun, reservedJob string
	acquired, err := withRuleTickLock(ctx, pool, lockName, func(ctx context.Context, tx pgx.Tx) error {
		taken, err := scheduledTickTaken(ctx, tx, ruleID, tick)
		if err != nil || taken {
			return err
		}
		reservedRun, reservedJob, err = insertRuleTickIntent(ctx, tx, ruleID, "SCHEDULED", scheduleTriggeredBy, "", nil, lockName)
		return err
	})
	if err != nil || !acquired || reservedRun == "" {
		return "", "", false, err
	}
	return reservedRun, reservedJob, true, nil
}

func scheduledTickTaken(ctx context.Context, tx pgx.Tx, ruleID string, tick time.Time) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM dms_svc.generation_run
			WHERE rule_id = $1::uuid
			  AND trigger_type = 'SCHEDULED'
			  AND started_at >= $2
			  AND started_at <= $3
		) OR EXISTS (
			SELECT 1
			FROM dms_svc.generation_job
			WHERE rule_id = $1::uuid
			  AND trigger_type = 'SCHEDULED'
			  AND is_deleted = false
			  AND created_at >= $2
			  AND created_at <= $3
		)`,
		ruleID,
		tick.Add(-time.Minute),
		tick.Add(time.Minute),
	).Scan(&exists)
	return exists, err
}

func reserveDateRelativeTick(
	ctx context.Context,
	pool *pgxpool.Pool,
	ruleID, sourceIDField string,
	sourceIDs []string,
	dayStart, dayEnd time.Time,
) (runID, jobID string, kept []string, claimed bool, err error) {
	lockName := ruleTickLockName("date", ruleID, dayStart)
	var reservedRun, reservedJob string
	acquired, err := withRuleTickLock(ctx, pool, lockName, func(ctx context.Context, tx pgx.Tx) error {
		fresh := make([]string, 0, len(sourceIDs))
		for _, sourceID := range sourceIDs {
			taken, err := dateSourceTaken(ctx, tx, ruleID, sourceID, dayStart, dayEnd)
			if err != nil {
				return err
			}
			if !taken {
				fresh = append(fresh, sourceID)
			}
		}
		if len(fresh) == 0 {
			return nil
		}
		var insertErr error
		reservedRun, reservedJob, insertErr = insertRuleTickIntent(
			ctx, tx, ruleID, "DATE_RELATIVE", dateRelativeActor, sourceIDField, fresh, lockName,
		)
		if insertErr != nil {
			return insertErr
		}
		kept = fresh
		return nil
	})
	if err != nil || !acquired || reservedRun == "" {
		return "", "", nil, false, err
	}
	return reservedRun, reservedJob, kept, true, nil
}

func dateSourceTaken(ctx context.Context, tx pgx.Tx, ruleID, sourceID string, dayStart, dayEnd time.Time) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM dms_svc.generation_run gr
			JOIN dms_svc.generation_run_source_row sr ON sr.run_id = gr.run_id
			WHERE gr.rule_id = $1::uuid
			  AND gr.trigger_type = 'DATE_RELATIVE'
			  AND sr.source_id = $2
			  AND gr.started_at >= $3
			  AND gr.started_at < $4
		) OR EXISTS (
			SELECT 1
			FROM dms_svc.generation_job
			WHERE rule_id = $1::uuid
			  AND trigger_type = 'DATE_RELATIVE'
			  AND is_deleted = false
			  AND created_at >= $3
			  AND created_at < $4
			  AND strpos(',' || source_ids_csv || ',', ',' || $2 || ',') > 0
		)`, ruleID, sourceID, dayStart, dayEnd).Scan(&exists)
	return exists, err
}

func insertRuleTickIntent(
	ctx context.Context,
	tx pgx.Tx,
	ruleID, triggerType, triggeredBy, sourceIDField string,
	sourceIDs []string,
	tickKey string,
) (runID, jobID string, err error) {
	var versionID, moduleCode, subModule string
	err = tx.QueryRow(ctx, `
		SELECT current_version_id::text, module_code, sub_module_code
		FROM dms_svc.generation_rule
		WHERE rule_id = $1::uuid AND is_deleted = false`, ruleID,
	).Scan(&versionID, &moduleCode, &subModule)
	if err != nil {
		return "", "", fmt.Errorf("load rule for tick intent: %w", err)
	}
	if strings.TrimSpace(versionID) == "" {
		return "", "", fmt.Errorf("rule %s has no current version", ruleID)
	}

	err = tx.QueryRow(ctx, `
		INSERT INTO dms_svc.generation_run (rule_id, version_id, trigger_type, triggered_by)
		VALUES ($1::uuid, $2::uuid, $3, $4)
		RETURNING run_id::text`,
		ruleID, versionID, triggerType, triggeredBy,
	).Scan(&runID)
	if err != nil {
		return "", "", fmt.Errorf("insert generation_run: %w", err)
	}

	cleaned := make([]string, 0, len(sourceIDs))
	for i, sourceID := range sourceIDs {
		sourceID = strings.TrimSpace(sourceID)
		if sourceID == "" {
			continue
		}
		cleaned = append(cleaned, sourceID)
		if _, err := tx.Exec(ctx, `
			INSERT INTO dms_svc.generation_run_source_row (run_id, source_id, sort_order)
			VALUES ($1::uuid, $2, $3)
			ON CONFLICT (run_id, source_id) DO NOTHING`, runID, sourceID, i); err != nil {
			return "", "", fmt.Errorf("insert generation_run_source_row: %w", err)
		}
	}

	if !docsvc.QueueEnabled() {
		return runID, "", nil
	}

	kind := "RULE_FULL"
	if strings.TrimSpace(sourceIDField) != "" && len(cleaned) > 0 {
		kind = "RULE_SOURCE_IDS"
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO dms_svc.generation_job (
		  job_kind, rule_id, version_id, trigger_type, triggered_by,
		  module_code, sub_module_code, source_id_field, source_ids_csv,
		  priority, status, available_at, max_attempts, request_id, run_id
		) VALUES (
		  $1, $2::uuid, $3::uuid, $4, $5,
		  $6, $7, $8, $9,
		  100, 'PENDING', now(), $10, $11, $12::uuid
		)
		RETURNING job_id::text`,
		kind, ruleID, versionID, triggerType, triggeredBy,
		moduleCode, subModule, strings.TrimSpace(sourceIDField), strings.Join(cleaned, ","),
		queueMaxAttempts(), fmt.Sprintf("%s:%s:%s", ruleID, triggerType, tickKey), runID,
	).Scan(&jobID)
	if err != nil {
		return "", "", fmt.Errorf("insert generation_job: %w", err)
	}
	return runID, jobID, nil
}
