package jobs

import (
	"context"
	"fmt"
	"time"

	"CimplrCorpSaas/internal/config"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

const (
	policyLifecycleLockName = "policy-lifecycle-sweeper"
	policyLifecycleActor    = "SYSTEM"
	// Daily shortly after midnight IST — flips Scheduled→Active and Active/Scheduled→Expired.
	policyLifecycleCronSpec = "5 0 * * *"
)

// PolicyLifecycleResult holds counts from one SweepPolicyLifecycleTransitions run.
type PolicyLifecycleResult struct {
	Activated int
	Expired   int
}

type lifecycleTransition struct {
	policyID  string
	oldStatus string
	newStatus string
}

// SweepPolicyLifecycleTransitions performs the two status transitions:
//   - Scheduled → Active where effective_start <= CURRENT_DATE
//   - Active|Scheduled → Expired where effective_end < CURRENT_DATE
//
// Expire runs first so a past-end Scheduled policy becomes Expired, not Active.
// Each transition writes an append-only policy_master_audit row (old_status/new_status)
// with action_type POLICY_ACTIVATED / POLICY_EXPIRED and requested_by = SYSTEM.
// Only rows with is_deleted = false are touched.
func SweepPolicyLifecycleTransitions(ctx context.Context, pool *pgxpool.Pool) (PolicyLifecycleResult, error) {
	var out PolicyLifecycleResult

	tx, err := pool.Begin(ctx)
	if err != nil {
		return out, fmt.Errorf("policy lifecycle begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	expired, err := applyLifecycleUpdate(ctx, tx, `
		WITH due AS (
			SELECT policy_id, status AS old_status
			FROM policyengine_svc.policy_master
			WHERE is_deleted = false
			  AND status IN ('Active', 'Scheduled')
			  AND effective_end IS NOT NULL
			  AND effective_end < CURRENT_DATE
			FOR UPDATE
		),
		upd AS (
			UPDATE policyengine_svc.policy_master p
			SET status = 'Expired',
			    last_modified_by = $1,
			    last_modified_at = now()
			FROM due
			WHERE p.policy_id = due.policy_id
			RETURNING due.policy_id::text AS policy_id, due.old_status, p.status AS new_status
		)
		SELECT policy_id, old_status, new_status FROM upd`, policyLifecycleActor)
	if err != nil {
		return out, fmt.Errorf("policy lifecycle expire: %w", err)
	}
	for _, t := range expired {
		if err := insertLifecycleAudit(ctx, tx, t.policyID, "POLICY_EXPIRED", t.oldStatus, t.newStatus); err != nil {
			return out, err
		}
	}
	out.Expired = len(expired)

	activated, err := applyLifecycleUpdate(ctx, tx, `
		WITH due AS (
			SELECT policy_id, status AS old_status
			FROM policyengine_svc.policy_master
			WHERE is_deleted = false
			  AND status = 'Scheduled'
			  AND effective_start <= CURRENT_DATE
			FOR UPDATE
		),
		upd AS (
			UPDATE policyengine_svc.policy_master p
			SET status = 'Active',
			    last_modified_by = $1,
			    last_modified_at = now()
			FROM due
			WHERE p.policy_id = due.policy_id
			RETURNING due.policy_id::text AS policy_id, due.old_status, p.status AS new_status
		)
		SELECT policy_id, old_status, new_status FROM upd`, policyLifecycleActor)
	if err != nil {
		return out, fmt.Errorf("policy lifecycle activate: %w", err)
	}
	for _, t := range activated {
		if err := insertLifecycleAudit(ctx, tx, t.policyID, "POLICY_ACTIVATED", t.oldStatus, t.newStatus); err != nil {
			return out, err
		}
	}
	out.Activated = len(activated)

	if err := tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("policy lifecycle commit: %w", err)
	}

	logger.LogInfo("[policy-lifecycle] activated=%d expired=%d", out.Activated, out.Expired)
	if logger.GlobalLogger != nil {
		logger.GlobalLogger.LogAudit(fmt.Sprintf(
			"Policy lifecycle sweeper: activated=%d expired=%d", out.Activated, out.Expired,
		))
	}
	return out, nil
}

func applyLifecycleUpdate(ctx context.Context, tx pgx.Tx, query string, actor string) ([]lifecycleTransition, error) {
	rows, err := tx.Query(ctx, query, actor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []lifecycleTransition
	for rows.Next() {
		var t lifecycleTransition
		if err := rows.Scan(&t.policyID, &t.oldStatus, &t.newStatus); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func insertLifecycleAudit(ctx context.Context, tx pgx.Tx, policyID, actionType, oldStatus, newStatus string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO policyengine_svc.policy_master_audit (
			policy_id, action_type, processing_status, requested_by,
			old_status, new_status
		) VALUES ($1::uuid, $2, 'APPROVED', $3, $4, $5)`,
		policyID, actionType, policyLifecycleActor, oldStatus, newStatus,
	)
	if err != nil {
		return fmt.Errorf("policy lifecycle audit %s for %s: %w", actionType, policyID, err)
	}
	return nil
}

// RunPolicyLifecycleSweeper schedules the daily lifecycle job shortly after midnight IST.
// The job body is wrapped in WithAdvisoryLock so multiple app instances cannot double-run.
func RunPolicyLifecycleSweeper(db *pgxpool.Pool) error {
	loc, err := time.LoadLocation(config.DefaultTimeZone)
	if err != nil {
		return fmt.Errorf("invalid timezone for policy lifecycle sweeper: %w", err)
	}

	c := cron.New(cron.WithLocation(loc))
	_, err = c.AddFunc(policyLifecycleCronSpec, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		ran := WithAdvisoryLock(ctx, db, policyLifecycleLockName, func() {
			if _, err := SweepPolicyLifecycleTransitions(ctx, db); err != nil {
				logger.LogError("[policy-lifecycle] sweep failed: %v", err)
				if logger.GlobalLogger != nil {
					logger.GlobalLogger.LogAudit("Policy lifecycle sweeper failed: " + err.Error())
				}
			}
		})
		if !ran && logger.GlobalLogger != nil {
			logger.GlobalLogger.LogAudit("Policy lifecycle sweeper skipped (advisory lock held)")
		}
	})
	if err != nil {
		return fmt.Errorf("failed to schedule policy lifecycle sweeper: %w", err)
	}

	c.Start()
	if logger.GlobalLogger != nil {
		logger.GlobalLogger.LogAudit(fmt.Sprintf(
			"Policy lifecycle sweeper scheduled daily at %s (%s)",
			policyLifecycleCronSpec, config.DefaultTimeZone,
		))
	}
	logger.LogInfo("Policy lifecycle sweeper scheduled: %s (%s)", policyLifecycleCronSpec, config.DefaultTimeZone)
	return nil
}
