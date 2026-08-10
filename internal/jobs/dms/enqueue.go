package dmsjobs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/services/docsvc"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EnqueueRuleGeneration checks Document-Service quota/hard-stop, then inserts a
// job into dms_svc.generation_job (client DB). Does not run render.
func EnqueueRuleGeneration(
	ctx context.Context,
	pool *pgxpool.Pool,
	ruleID, triggerType, triggeredBy, sourceIDField string,
	sourceIDs []string,
) (jobID string, err error) {
	if pool == nil {
		return "", fmt.Errorf("pool required")
	}
	client := docsvc.NewFromEnv()
	q, err := client.QuotaCheck(ctx)
	if err != nil {
		return "", fmt.Errorf("document-service quota check: %w", err)
	}
	if !q.Allowed {
		code := q.ErrorCode
		if code == "" {
			code = "DMS_GENERATION_QUOTA_EXCEEDED"
		}
		return "", fmt.Errorf("%s: %s", code, q.Message)
	}

	rule, err := loadRuleHeader(ctx, pool, ruleID)
	if err != nil {
		return "", fmt.Errorf("load rule: %w", err)
	}
	if rule.ProcessingStatus != "APPROVED" || rule.Status != "Active" || rule.CurrentVersionID == "" {
		return "", fmt.Errorf("rule %s is not an approved, active rule with a live version", ruleID)
	}

	kind := "RULE_FULL"
	if strings.TrimSpace(sourceIDField) != "" && len(sourceIDs) > 0 {
		kind = "RULE_SOURCE_IDS"
	}
	reqID := fmt.Sprintf("%s:%s:%d", rule.RuleID, triggerType, time.Now().UnixNano())
	jobID, err = insertGenerationJob(ctx, pool, generationJobInsert{
		Kind:          kind,
		RuleID:        rule.RuleID,
		VersionID:     rule.CurrentVersionID,
		TriggerType:   triggerType,
		TriggeredBy:   triggeredBy,
		ModuleCode:    rule.ModuleCode,
		SubModuleCode: rule.SubModuleCode,
		SourceIDField: sourceIDField,
		RequestID:     reqID,
		SourceIDs:     sourceIDs,
	})
	if err != nil {
		return "", fmt.Errorf("enqueue job: %w", err)
	}
	return jobID, nil
}

// StartGenerationQueueWorker claims jobs from dms_svc, runs existing generation
// cores, bumps Document-Service quota, then finishes the local job.
// Dispatch/email already happen inside RunGeneration* on success.
func StartGenerationQueueWorker(ctx context.Context, pool *pgxpool.Pool) {
	if !docsvc.QueueEnabled() {
		api.LogInfo("[DMS-QUEUE] DMS_GENERATION_QUEUE_ENABLED not set — queue worker not started")
		return
	}
	client := docsvc.NewFromEnv()
	owner, _ := os.Hostname()
	if owner == "" {
		owner = "cimplr-cron"
	}
	owner = "main-" + owner
	poll := 2 * time.Second
	if v := strings.TrimSpace(os.Getenv("DMS_GENERATION_WORKER_POLL_MS")); v != "" {
		if ms, err := time.ParseDuration(v + "ms"); err == nil && ms > 0 {
			poll = ms
		}
	}
	api.LogInfo("[DMS-QUEUE] generation queue worker started owner=%s poll=%s", owner, poll)
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			q, err := client.QuotaCheck(ctx)
			if err != nil {
				api.LogError("[DMS-QUEUE] quota check: %v", err)
				continue
			}
			if !q.Allowed {
				continue
			}
			jobs, err := claimGenerationJobs(ctx, pool, owner, queueBatchSize())
			if err != nil {
				api.LogError("[DMS-QUEUE] claim: %v", err)
				continue
			}
			for _, job := range jobs {
				executeClaimedJob(ctx, pool, job)
			}
		}
	}
}

func executeClaimedJob(ctx context.Context, pool *pgxpool.Pool, job generationJob) {
	api.LogInfo("[DMS-QUEUE] executing job=%s kind=%s rule=%s", job.JobID, job.JobKind, job.RuleID)

	var runID string
	var runErr error
	switch strings.ToUpper(job.JobKind) {
	case "RULE_SOURCE_IDS":
		runID, runErr = RunGenerationForSourceIDs(ctx, pool, job.RuleID, job.TriggerType, job.TriggeredBy, job.SourceIDField, job.SourceIDs)
	case "ADHOC":
		runErr = fmt.Errorf("ADHOC queue execute not wired yet — use sync adhoc path")
	default:
		runID, runErr = RunGeneration(ctx, pool, job.RuleID, job.TriggerType, job.TriggeredBy)
	}

	if runErr != nil {
		api.LogError("[DMS-QUEUE] job=%s failed: %v", job.JobID, runErr)
		_ = finishGenerationJob(ctx, pool, job.JobID, false, runID, runErr.Error(), job.AttemptCount < job.MaxAttempts)
		return
	}
	// Quota increment + DispatchRun already happen inside RunGeneration*.
	if err := finishGenerationJob(ctx, pool, job.JobID, true, runID, "", false); err != nil {
		api.LogError("[DMS-QUEUE] finish job=%s: %v", job.JobID, err)
	}
	api.LogInfo("[DMS-QUEUE] job=%s done run=%s", job.JobID, runID)
}
