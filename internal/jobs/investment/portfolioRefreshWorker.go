package jobs

import (
	"context"
	"fmt"
	"time"

	"CimplrCorpSaas/api/investment/portfolio"
	"CimplrCorpSaas/internal/config"
	"CimplrCorpSaas/internal/logger"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

// RefreshPortfolioSnapshotsJob rebuilds portfolio snapshots in the DB.
// Pass entityNames = nil (or empty) to refresh ALL entities.
// Called by the daily cron and by event-driven hooks (confirmation, redemption confirmation, onboard).
func RefreshPortfolioSnapshotsJob(ctx context.Context, db *pgxpool.Pool, entityNames []string) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if len(entityNames) > 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM investment.portfolio_snapshot WHERE entity_name = ANY($1::text[])`, entityNames); err != nil {
			return fmt.Errorf("delete snapshots: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `DELETE FROM investment.portfolio_snapshot`); err != nil {
			return fmt.Errorf("delete all snapshots: %w", err)
		}
	}

	batchID := uuid.New().String()

	var entityArg interface{}
	if len(entityNames) > 0 {
		entityArg = entityNames
	}

	if _, err := tx.Exec(ctx, portfolio.PortfolioSnapshotInsertSQL, entityArg, batchID); err != nil {
		return fmt.Errorf("insert snapshots: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	logger.GlobalLogger.LogAudit(fmt.Sprintf("Portfolio snapshot refresh complete (batch=%s, entities=%v)", batchID, entityNames))
	return nil
}

// RunPortfolioRefreshWorker schedules daily portfolio snapshot refresh at 9 AM IST.
func RunPortfolioRefreshWorker(db *pgxpool.Pool) error {
	loc, err := time.LoadLocation(config.DefaultTimeZone)
	if err != nil {
		return fmt.Errorf("invalid timezone for portfolio refresh worker: %w", err)
	}

	c := cron.New(cron.WithLocation(loc))

	// Daily at 09:00 IST — after AMFI NAV data is typically updated
	_, err = c.AddFunc("0 9 * * *", func() {
		logger.GlobalLogger.LogAudit("Portfolio refresh worker: starting daily run at " + time.Now().In(loc).String())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		if err := RefreshPortfolioSnapshotsJob(ctx, db, nil); err != nil {
			logger.GlobalLogger.LogAudit("Portfolio refresh worker: daily run failed: " + err.Error())
		} else {
			logger.GlobalLogger.LogAudit("Portfolio refresh worker: daily run completed at " + time.Now().In(loc).String())
		}
	})
	if err != nil {
		return fmt.Errorf("failed to schedule portfolio refresh cron: %w", err)
	}

	c.Start()
	logger.GlobalLogger.LogAudit("Portfolio refresh worker scheduled daily at 09:00 IST")
	return nil
}
