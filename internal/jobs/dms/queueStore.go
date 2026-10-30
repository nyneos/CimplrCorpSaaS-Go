package dmsjobs

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Jobs this process has claimed and not yet finished. reclaimExpiredLeases
// skips them so a slow batch is not flipped back to PENDING while it is still
// running here. The lease heartbeat is what keeps other processes from
// reclaiming those rows; this set is only same-process safety.
var (
	activeLeaseMu sync.Mutex
	activeLeases  = map[string]*generationLeaseHold{}
)

type generationLeaseHold struct {
	cancel context.CancelFunc
}

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

// generationJobInsert is one new dms_svc.generation_job row: what to generate
// (kind + rule/version + optional source ids), who asked for it, and how it is
// prioritised. Priority <= 0 means "derive from trigger type / kind".
type generationJobInsert struct {
	Kind          string
	RuleID        string
	VersionID     string
	TriggerType   string
	TriggeredBy   string
	ModuleCode    string
	SubModuleCode string
	SourceIDField string
	RequestID     string
	SourceIDs     []string
	Priority      int
}

func insertGenerationJob(ctx context.Context, pool *pgxpool.Pool, j generationJobInsert) (jobID string, err error) {
	priority := j.Priority
	if priority <= 0 {
		priority = 100
		if strings.EqualFold(j.TriggerType, "MANUAL") || j.Kind == "ADHOC" {
			priority = 50
		}
	}
	csv := strings.Join(j.SourceIDs, ",")
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
		j.Kind, j.RuleID, j.VersionID, j.TriggerType, j.TriggeredBy,
		j.ModuleCode, j.SubModuleCode, j.SourceIDField, csv,
		priority, queueMaxAttempts(), j.RequestID,
	).Scan(&jobID)
	return jobID, err
}

func trackActiveGenerationLease(jobID string) {
	if jobID == "" {
		return
	}
	activeLeaseMu.Lock()
	if _, ok := activeLeases[jobID]; !ok {
		activeLeases[jobID] = &generationLeaseHold{}
	}
	activeLeaseMu.Unlock()
}

func untrackActiveGenerationLease(jobID string) {
	activeLeaseMu.Lock()
	delete(activeLeases, jobID)
	activeLeaseMu.Unlock()
}

func generationLeaseTracked(jobID string) bool {
	activeLeaseMu.Lock()
	_, ok := activeLeases[jobID]
	activeLeaseMu.Unlock()
	return ok
}

func setActiveGenerationLeaseCancel(jobID string, cancel context.CancelFunc) {
	activeLeaseMu.Lock()
	if h, ok := activeLeases[jobID]; ok {
		h.cancel = cancel
	}
	activeLeaseMu.Unlock()
}

func clearActiveGenerationLeaseCancel(jobID string) {
	setActiveGenerationLeaseCancel(jobID, nil)
}

// cancelRunningGenerationLease aborts the in-flight run when the lease row is
// no longer ours. Returns false when this process is not currently executing
// that job (already finished, or still waiting to start).
func cancelRunningGenerationLease(jobID string) bool {
	activeLeaseMu.Lock()
	h, ok := activeLeases[jobID]
	if !ok || h.cancel == nil {
		activeLeaseMu.Unlock()
		return false
	}
	cancel := h.cancel
	delete(activeLeases, jobID)
	activeLeaseMu.Unlock()
	cancel()
	return true
}

func activeGenerationLeaseIDs() []string {
	activeLeaseMu.Lock()
	defer activeLeaseMu.Unlock()
	ids := make([]string, 0, len(activeLeases))
	for id := range activeLeases {
		ids = append(ids, id)
	}
	return ids
}

func leaseHeartbeatInterval() time.Duration {
	interval := time.Duration(queueLeaseSeconds()) * time.Second / 3
	if interval < time.Second {
		interval = time.Second
	}
	return interval
}

// renewGenerationLease pushes lease_expires_at out for the row this owner
// still holds. owned is false when no row matched (lost lease or already finished).
func renewGenerationLease(ctx context.Context, pool *pgxpool.Pool, jobID, owner string) (owned bool, err error) {
	if pool == nil || strings.TrimSpace(jobID) == "" || strings.TrimSpace(owner) == "" {
		return false, fmt.Errorf("lease renew: job and owner required")
	}
	lease := fmt.Sprintf("%d seconds", queueLeaseSeconds())
	tag, err := pool.Exec(ctx, `
		UPDATE dms_svc.generation_job SET
		  lease_expires_at = now() + $3::interval
		WHERE job_id = $1::uuid
		  AND lease_owner = $2
		  AND status = 'LEASED'
		  AND is_deleted = false`, jobID, owner, lease)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func renewActiveGenerationLeases(ctx context.Context, pool *pgxpool.Pool, owner string) {
	for _, id := range activeGenerationLeaseIDs() {
		owned, err := renewGenerationLease(ctx, pool, id, owner)
		if err != nil {
			api.LogError("[DMS-QUEUE] lease heartbeat job=%s: %v", id, err)
			continue
		}
		if owned {
			continue
		}
		if cancelRunningGenerationLease(id) {
			api.LogError("[DMS-QUEUE] lease heartbeat job=%s owner=%s updated 0 rows — lease lost, stopping run", id, owner)
		}
	}
}

// startGenerationLeaseHeartbeat extends every lease this process still holds
// until stop is called. The first renew runs immediately so a job waiting
// behind a long sibling does not sit on the original claim expiry.
func startGenerationLeaseHeartbeat(ctx context.Context, pool *pgxpool.Pool, owner string) func() {
	ticker := time.NewTicker(leaseHeartbeatInterval())
	done := make(chan struct{})
	go func() {
		defer ticker.Stop()
		renewActiveGenerationLeases(ctx, pool, owner)
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				renewActiveGenerationLeases(ctx, pool, owner)
			}
		}
	}()
	return func() { close(done) }
}

func reclaimExpiredLeases(ctx context.Context, pool *pgxpool.Pool) {
	skip := activeGenerationLeaseIDs()
	if skip == nil {
		skip = []string{}
	}
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
		  AND lease_expires_at < now()
		  AND job_id::text <> ALL($1::text[])`, skip)
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

// finishGenerationJob writes the terminal status only when this worker still
// owns the row (same lease_owner and attempt_count it claimed). A stale
// worker whose lease was reclaimed updates 0 rows and must not overwrite the
// newer attempt. errMsg is stored in last_error; pass "" on a clean success.
func finishGenerationJob(ctx context.Context, pool *pgxpool.Pool, jobID, leaseOwner string, attempt int, success bool, runID, errMsg string, retry bool) error {
	var updated int64
	var err error
	switch {
	case success:
		updated, err = execGenerationJob(ctx, pool, `
			UPDATE dms_svc.generation_job SET
			  status = 'DONE',
			  finished_at = now(),
			  last_error = $4,
			  run_id = COALESCE(NULLIF($2,'')::uuid, run_id),
			  lease_owner = NULL,
			  leased_at = NULL,
			  lease_expires_at = NULL
			WHERE job_id = $1::uuid
			  AND lease_owner = $3
			  AND attempt_count = $5
			  AND status = 'LEASED'
			  AND is_deleted = false`, jobID, runID, leaseOwner, errMsg, attempt)
	case retry:
		updated, err = execGenerationJob(ctx, pool, `
			UPDATE dms_svc.generation_job SET
			  status = 'DELAYED',
			  available_at = now() + interval '60 seconds',
			  lease_owner = NULL,
			  leased_at = NULL,
			  lease_expires_at = NULL,
			  last_error = $4
			WHERE job_id = $1::uuid
			  AND lease_owner = $2
			  AND attempt_count = $3
			  AND status = 'LEASED'
			  AND is_deleted = false`, jobID, leaseOwner, attempt, errMsg)
	default:
		updated, err = execGenerationJob(ctx, pool, `
			UPDATE dms_svc.generation_job SET
			  status = 'DEAD',
			  finished_at = now(),
			  last_error = $4,
			  lease_owner = NULL,
			  leased_at = NULL,
			  lease_expires_at = NULL
			WHERE job_id = $1::uuid
			  AND lease_owner = $2
			  AND attempt_count = $3
			  AND status = 'LEASED'
			  AND is_deleted = false`, jobID, leaseOwner, attempt, errMsg)
	}
	if err != nil {
		return err
	}
	if updated == 0 {
		api.LogError("[DMS-QUEUE] finish job=%s owner=%s attempt=%d updated 0 rows — lease lost, leaving the row unchanged", jobID, leaseOwner, attempt)
		return fmt.Errorf("generation job %s finish skipped: lease not owned by %s at attempt %d", jobID, leaseOwner, attempt)
	}
	return nil
}

func execGenerationJob(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (int64, error) {
	tag, err := pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
