package dmsjobs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	dmscommon "CimplrCorpSaas/api/dms/common"
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
	if !dmscommon.IsDMSEnabled() {
		return "", fmt.Errorf("DMS_DISABLED: DMS is disabled at application level")
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
			if !dmscommon.IsDMSEnabled() {
				continue
			}
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
			if len(jobs) == 0 {
				continue
			}
			runClaimedGenerationBatch(ctx, pool, owner, jobs)
		}
	}
}

func runClaimedGenerationBatch(ctx context.Context, pool *pgxpool.Pool, owner string, jobs []generationJob) {
	for _, job := range jobs {
		trackActiveGenerationLease(job.JobID)
	}
	stopHeartbeat := startGenerationLeaseHeartbeat(ctx, pool, owner)
	defer func() {
		for _, job := range jobs {
			untrackActiveGenerationLease(job.JobID)
		}
	}()
	defer stopHeartbeat()
	for _, job := range jobs {
		executeClaimedJob(ctx, pool, owner, job)
		untrackActiveGenerationLease(job.JobID)
	}
}

func executeClaimedJob(ctx context.Context, pool *pgxpool.Pool, owner string, job generationJob) {
	if !generationLeaseTracked(job.JobID) {
		api.LogError("[DMS-QUEUE] job=%s dropped before run — lease lost while waiting", job.JobID)
		return
	}
	owned, err := renewGenerationLease(ctx, pool, job.JobID, owner)
	if err != nil {
		api.LogError("[DMS-QUEUE] job=%s lease renew before run: %v", job.JobID, err)
	} else if !owned {
		api.LogError("[DMS-QUEUE] job=%s lease lost before run — not executing", job.JobID)
		untrackActiveGenerationLease(job.JobID)
		return
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	setActiveGenerationLeaseCancel(job.JobID, cancel)
	defer clearActiveGenerationLeaseCancel(job.JobID)

	api.LogInfo("[DMS-QUEUE] executing job=%s kind=%s rule=%s", job.JobID, job.JobKind, job.RuleID)

	var runID string
	var runErr error
	claimedRunID := strings.TrimSpace(job.RunID)
	switch strings.ToUpper(job.JobKind) {
	case "RULE_SOURCE_IDS":
		runID, runErr = runGeneration(runCtx, pool, job.RuleID, job.TriggerType, job.TriggeredBy, job.SourceIDField, job.SourceIDs, claimedRunID)
	case "ADHOC":
		runErr = fmt.Errorf("ADHOC queue execute not wired yet — use sync adhoc path")
	default:
		runID, runErr = runGeneration(runCtx, pool, job.RuleID, job.TriggerType, job.TriggeredBy, "", nil, claimedRunID)
	}

	clearActiveGenerationLeaseCancel(job.JobID)

	if runErr != nil {
		var dispatchMiss *dispatchAfterGenerationError
		if errors.As(runErr, &dispatchMiss) {
			api.LogError("[DMS-QUEUE] job=%s run=%s generation done, dispatch failed — not requeueing generation: %v", job.JobID, runID, runErr)
			if err := finishGenerationJob(ctx, pool, job.JobID, owner, job.AttemptCount, true, runID, runErr.Error(), false); err != nil {
				api.LogError("[DMS-QUEUE] finish job=%s after dispatch failure: %v", job.JobID, err)
				return
			}
			api.LogInfo("[DMS-QUEUE] job=%s done run=%s (dispatch pending retry)", job.JobID, runID)
			return
		}
		api.LogError("[DMS-QUEUE] job=%s failed: %v", job.JobID, runErr)
		if err := finishGenerationJob(ctx, pool, job.JobID, owner, job.AttemptCount, false, runID, runErr.Error(), job.AttemptCount < job.MaxAttempts); err != nil {
			api.LogError("[DMS-QUEUE] finish job=%s: %v", job.JobID, err)
		}
		return
	}
	if err := finishGenerationJob(ctx, pool, job.JobID, owner, job.AttemptCount, true, runID, "", false); err != nil {
		api.LogError("[DMS-QUEUE] finish job=%s: %v", job.JobID, err)
		return
	}
	api.LogInfo("[DMS-QUEUE] job=%s done run=%s", job.JobID, runID)
}
