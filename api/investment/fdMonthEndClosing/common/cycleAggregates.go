package common

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// RefreshCycleFdCount sets fd_closing_cycle.fd_count to the live count of
// non-deleted APPROVED scope rows for cycleID. Cached column was never
// written after scope approve — list/detail now also compute live, but this
// keeps the master row honest for any reader still using the column.
func RefreshCycleFdCount(ctx context.Context, tx pgx.Tx, cycleID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE investment.fd_closing_cycle c
		SET fd_count = COALESCE((
			SELECT COUNT(*)::int
			FROM investment.fd_closing_cycle_fd_scope s
			WHERE s.cycle_id = c.cycle_id
			  AND s.is_deleted = false
			  AND s.selection_status = 'APPROVED'
		), 0),
		    updated_at = now()
		WHERE c.cycle_id = $1`,
		cycleID,
	)
	return err
}

// RefreshCycleReadiness recomputes readiness_score / blocker_count / eligibility
// from checklist items for cycleID (same rule as checklist.recomputeCycleReadiness).
func RefreshCycleReadiness(ctx context.Context, tx pgx.Tx, cycleID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE investment.fd_closing_cycle c
		SET readiness_score      = COALESCE(agg.readiness_score, 0),
		    blocker_count        = COALESCE(agg.blocker_count, 0),
		    eligibility          = CASE
		        WHEN COALESCE(agg.total_count, 0) = 0 THEN 'NOT_READY'
		        WHEN agg.completed_count = agg.total_count THEN 'READY_TO_CLOSE'
		        WHEN agg.critical_incomplete = 0 THEN 'CONDITIONALLY_READY'
		        ELSE 'NOT_READY'
		    END,
		    readiness_checked_at = now(),
		    updated_at           = now()
		FROM (
			SELECT
				$1::text AS cycle_id,
				COUNT(*) AS total_count,
				COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed_count,
				COUNT(*) FILTER (WHERE status = 'BLOCKED') AS blocker_count,
				CASE WHEN COUNT(*) = 0 THEN 0
				     ELSE ROUND(COUNT(*) FILTER (WHERE status = 'COMPLETED') * 100.0 / COUNT(*), 2)
				END AS readiness_score,
				COUNT(*) FILTER (WHERE is_critical = true AND status <> 'COMPLETED') AS critical_incomplete
			FROM investment.fd_closing_checklist_item
			WHERE cycle_id = $1
		) agg
		WHERE c.cycle_id = $1`,
		cycleID,
	)
	return err
}
