package dmsjobs

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Local generation queue in dms_svc (client DB). Document-Service never sees these rows.

type generationJob struct {
	JobID          string
	RunID          string
	JobKind        string
	RuleID         string
	VersionID      string
	TriggerType    string
	TriggeredBy    string
	ModuleCode     string
	SubModuleCode  string
	SourceIDField  string
	SourceIDs      []string
	AdhocRequestID string
	AttemptCount   int
	MaxAttempts    int
	Status         string
	RequestID      string
}

func queueMaxAttempts() int {
	v := strings.TrimSpace(os.Getenv("DMS_GENERATION_MAX_ATTEMPTS"))
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return n
	}
	return 3
}

func queueLeaseSeconds() int {
	v := strings.TrimSpace(os.Getenv("DMS_GENERATION_LEASE_SECONDS"))
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return n
	}
	return 600
}

func queueBatchSize() int {
	v := strings.TrimSpace(os.Getenv("DMS_GENERATION_BATCH_SIZE"))
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return n
	}
	return 2
}

func queueMaxConcurrent() int {
	v := strings.TrimSpace(os.Getenv("DMS_GENERATION_MAX_CONCURRENT"))
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return n
	}
	return 2
}

func insertGenerationJob(
	ctx context.Context,
	pool *pgxpool.Pool,
	kind, ruleID, versionID, triggerType, triggeredBy, moduleCode, subModuleCode, sourceIDField, requestID string,
	sourceIDs []string,
	priority int,
) (jobID string, err error) {
	if priority <= 0 {
		priority = 100
		if strings.EqualFold(triggerType, "MANUAL") || kind == "ADHOC" {
			priority = 50
		}
	}
	csv := strings.Join(sourceIDs, ",")
	err = pool.QueryRow(ctx, `
		INSERT INTO dms_svc.generation_job (
		  job_kind, rule_id, version_id, trigger_type, triggered_by,
		  module_code, sub_module_code, source_id_field, source_ids_csv,
		  priority, status, available_at, max_attempts, request_id
		) VALUES (
		  $1, NULLIF($2,'')::uuid, NULLIF($3,'')::uuid, $4, $5,
		  $6, $7, $8, $9,
		  $10, 'PENDING', now(), $11, $12
		)
		RETURNING job_id::text`,
		kind, ruleID, versionID, triggerType, triggeredBy,
		moduleCode, subModuleCode, sourceIDField, csv,
		priority, queueMaxAttempts(), requestID,
	).Scan(&jobID)
	return jobID, err
}

func reclaimExpiredLeases(ctx context.Context, pool *pgxpool.Pool) {
	_, _ = pool.Exec(ctx, `
		UPDATE dms_svc.generation_job SET
		  status = 'PENDING',
		  leased_at = NULL,
		  lease_expires_at = NULL,
		  lease_owner = NULL,
		  available_at = now()
		WHERE is_deleted = false
		  AND status = 'LEASED'
		  AND lease_expires_at IS NOT NULL
		  AND lease_expires_at < now()`)
}

func countLeased(ctx context.Context, pool *pgxpool.Pool) int {
	var n int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM dms_svc.generation_job
		WHERE is_deleted = false
		  AND status = 'LEASED'
		  AND (lease_expires_at IS NULL OR lease_expires_at > now())`).Scan(&n)
	return n
}

func claimGenerationJobs(ctx context.Context, pool *pgxpool.Pool, owner string, limit int) ([]generationJob, error) {
	reclaimExpiredLeases(ctx, pool)
	slots := queueMaxConcurrent() - countLeased(ctx, pool)
	if slots <= 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = queueBatchSize()
	}
	if limit > slots {
		limit = slots
	}
	if limit > queueBatchSize() {
		limit = queueBatchSize()
	}
	lease := fmt.Sprintf("%d seconds", queueLeaseSeconds())

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		WITH cte AS (
		  SELECT job_id FROM dms_svc.generation_job
		  WHERE is_deleted = false
		    AND status IN ('PENDING', 'DELAYED')
		    AND available_at <= now()
		  ORDER BY priority ASC, created_at ASC
		  FOR UPDATE SKIP LOCKED
		  LIMIT $1
		)
		UPDATE dms_svc.generation_job j SET
		  status = 'LEASED',
		  leased_at = now(),
		  lease_expires_at = now() + $2::interval,
		  lease_owner = $3,
		  attempt_count = attempt_count + 1
		FROM cte WHERE j.job_id = cte.job_id
		RETURNING j.job_id::text, COALESCE(j.run_id::text,''), j.job_kind,
		  COALESCE(j.rule_id::text,''), COALESCE(j.version_id::text,''),
		  j.trigger_type, j.triggered_by, j.module_code, j.sub_module_code,
		  j.source_id_field, j.source_ids_csv, COALESCE(j.adhoc_request_id::text,''),
		  j.attempt_count, j.max_attempts, j.status, j.request_id`,
		limit, lease, owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]generationJob, 0, limit)
	for rows.Next() {
		var j generationJob
		var csv string
		if err := rows.Scan(
			&j.JobID, &j.RunID, &j.JobKind, &j.RuleID, &j.VersionID,
			&j.TriggerType, &j.TriggeredBy, &j.ModuleCode, &j.SubModuleCode,
			&j.SourceIDField, &csv, &j.AdhocRequestID,
			&j.AttemptCount, &j.MaxAttempts, &j.Status, &j.RequestID,
		); err != nil {
			return nil, err
		}
		if csv != "" {
			j.SourceIDs = strings.Split(csv, ",")
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func finishGenerationJob(ctx context.Context, pool *pgxpool.Pool, jobID string, success bool, runID, errMsg string, retry bool) error {
	if success {
		_, err := pool.Exec(ctx, `
			UPDATE dms_svc.generation_job SET
			  status = 'DONE', finished_at = now(), last_error = '',
			  run_id = COALESCE(NULLIF($2,'')::uuid, run_id)
			WHERE job_id = $1::uuid`, jobID, runID)
		return err
	}
	if retry {
		_, err := pool.Exec(ctx, `
			UPDATE dms_svc.generation_job SET
			  status = 'DELAYED',
			  available_at = now() + interval '60 seconds',
			  lease_owner = NULL,
			  leased_at = NULL,
			  lease_expires_at = NULL,
			  last_error = $2
			WHERE job_id = $1::uuid`, jobID, errMsg)
		return err
	}
	_, err := pool.Exec(ctx, `
		UPDATE dms_svc.generation_job SET
		  status = 'DEAD', finished_at = now(), last_error = $2
		WHERE job_id = $1::uuid`, jobID, errMsg)
	return err
}
