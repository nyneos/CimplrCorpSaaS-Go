package jobs

import (
	"context"
	"time"

	fdclosure "CimplrCorpSaas/api/investment/fdMaturityAndRollover"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StartCimplrAutoMaturityWorker runs daily (and once at startup) to auto-create and
// approve payout/rollover confirms for approved initiates on or after FD maturity date.
func StartCimplrAutoMaturityWorker(db *pgxpool.Pool) {
	runCimplrAutoMaturity(db)
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		runCimplrAutoMaturity(db)
	}
}

func runCimplrAutoMaturity(db *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	logger.LogInfo("[CimplrAutoMaturity] run started at %s", time.Now().Format(time.RFC3339))
	ok, skip, fail := fdclosure.RunCimplrAutoMaturityDue(ctx, db)
	logger.LogInfo("[CimplrAutoMaturity] done processed=%d skipped=%d failed=%d", ok, skip, fail)
}
