package dmsjobs

import (
	"context"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/config"
	"CimplrCorpSaas/internal/services/docsvc"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

const (
	scheduleWorkerPoll   = 60 * time.Second
	scheduleTriggeredBy  = "SYSTEM:dms-scheduler"
	scheduleCronSpecBits = cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor
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

		alreadyFired, err := hasScheduledRunForTick(ctx, pool, rule.RuleID, tick)
		if err != nil {
			api.LogError("[DMS-SCHEDULE] rule=%s idempotency check: %v", rule.RuleID, err)
			continue
		}
		if alreadyFired {
			continue
		}

		if docsvc.QueueEnabled() {
			jobID, err := EnqueueRuleGeneration(ctx, pool, rule.RuleID, "SCHEDULED", scheduleTriggeredBy, "", nil)
			if err != nil {
				api.LogError("[DMS-SCHEDULE] rule=%s tick=%s enqueue: %v", rule.RuleID, tick.Format(time.RFC3339), err)
				continue
			}
			api.LogInfo("[DMS-SCHEDULE] rule=%s tick=%s job=%s queued", rule.RuleID, tick.Format(time.RFC3339), jobID)
			continue
		}
		runID, err := RunGeneration(ctx, pool, rule.RuleID, "SCHEDULED", scheduleTriggeredBy)
		if err != nil {
			api.LogError("[DMS-SCHEDULE] rule=%s tick=%s run=%s: %v", rule.RuleID, tick.Format(time.RFC3339), runID, err)
			continue
		}
		api.LogInfo("[DMS-SCHEDULE] rule=%s tick=%s run=%s triggered", rule.RuleID, tick.Format(time.RFC3339), runID)
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

func hasScheduledRunForTick(ctx context.Context, pool *pgxpool.Pool, ruleID string, tick time.Time) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM dms_svc.generation_run
			WHERE rule_id = $1::uuid
			  AND trigger_type = 'SCHEDULED'
			  AND started_at >= $2
			  AND started_at <= $3
		)`,
		ruleID,
		tick.Add(-time.Minute),
		tick.Add(time.Minute),
	).Scan(&exists)
	return exists, err
}
